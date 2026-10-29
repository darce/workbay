#!/usr/bin/env bash
# Remote agent build (grok) in a hardened sandbox on the OCI VM.
#
# Runs a grok-cli AGENTIC build for a committed branch inside a
# HISTORY-STRIPPED, REMOTE-SEVERED sandbox on the remote host, resource-capped,
# and returns the resulting change as a git patch on stdout (or --out FILE). The
# caller applies + verifies the patch locally — the remote sandbox is never the
# source of truth (fetch-back is a patch, not a `git fetch`: the sandbox has no
# remote to fetch from, by design).
#
# Prototype for implementation note S2. Flow proven end-to-end 2026-07-15 (decisions
# #3871 grok-authed, #3872 operator sandbox posture, #3873 first sandboxed build
# -> commit c40cabd2).
#
# Usage:
#   scripts/remote_agent.sh build --branch <br> --brief <file> --schema <file> \
#   scripts/remote_agent.sh submit --job-id <j+20hex> --branch <br> --brief <file> --schema <file> \
#       --agent-spec <file.json>   # same flags as build; returns JSON {job_id,state,unit}
#       --agent-spec <file.json> [--out <patch>] \
#       [--result-out <json>] [--debug-out <log>] [--stream-out <jsonl>] [--timeout <seconds>]
#       [--uncommitted-out <patch>]
#   scripts/remote_agent.sh doctor          # grok readiness on the host
#   scripts/remote_agent.sh reap [--idle-seconds N] [--dry-run] [--authorized-only] [--eligible-keys-stdin] [--eligible-key K]...
#   scripts/remote_agent.sh status [--sweep] --job-id <j+20hex> [--job-id ...]
#   scripts/remote_agent.sh status --list   # read-only: one JSON row per VM job dir,
#                      typed error rows for bad dirs; refuses --sweep and --job-id.
#                      Bounds from the environment, forwarded to the VM when set
#                      (positive integers): STATUS_LIST_MAX rows (default 1000,
#                      ceiling 10000), STATUS_LIST_DEADLINE_S seconds (default 90,
#                      ceiling 600). Exit 3 = truncated by a bound, 1 = could not list.
#   scripts/remote_agent.sh collect --job-id <j+20hex>
#   scripts/remote_agent.sh collect --job-id <j+20hex> --stage-dir <existing private dir>
#   scripts/remote_agent.sh cancel --job-id <j+20hex>
#
#   --out FILE         write grok's committed changes as a git patch to FILE.
#   --result-out FILE  write grok's structured stdout JSON (the BackendResult
#                      envelope) to FILE, best-effort: fetched even on a no-change /
#                      grok-fail exit so the caller can surface grok's summary/blockers.
#   --debug-out FILE   write grok's --debug-file log to FILE, best-effort: lets the
#                      caller run the post-turn grok-build contamination backstop
#                      (decision #2799 pin guard) on the same evidence GrokCliAdapter uses.
#   --stream-out FILE  fetch the nonce-scoped agent event stream to FILE, best-effort.
#   --test-cmd CMD     off-box self-verify (item 26): after grok commits, run CMD in the
#                      sandbox venv, capturing {command,exit_code,passed,output_tail} JSON.
#                      The patch is still emitted regardless of the result — the caller
#                      (worker) gates on the captured outcome; this script only measures.
#   --selfverify-out FILE  fetch the off-box self-verify JSON to FILE, best-effort (only
#                      written when --test-cmd was supplied and grok committed).
#   --phases-out FILE  fetch the per-dispatch phase-timing record (.agent-phases.json) to
#                      FILE, best-effort (implementation note S1). Fetched unconditionally w.r.t.
#                      exit class so a post-materialize partial still lands on the host;
#                      miss is degrade, never a dispatch error [OBS-08][fail-open].
#   --uncommitted-out FILE  fetch the nonce-scoped uncommitted remainder patch to FILE,
#                      best-effort. The file is separate from --out so a raw working-tree
#                      diff cannot corrupt the committed format-patch stream.
#   --provenance-out FILE  fetch the gate-authored source-to-sandbox receipt. The
#                      receipt is nonce-scoped remotely and remains in the local
#                      durable spool after sandbox/ref cleanup.
#   --timeout SECONDS  hard wall-clock budget for the remote turn (0 = none), measured
#                      from build start. Pre-dispatch probe + push + scp consume part
#                      of this budget; residual = max(0, budget − elapsed) is applied
#                      to remote grok (never floored above the remaining budget — when
#                      residual hits 0 the turn fails fast before grok starts). Caller
#                      should already subtract post-grok fetch headroom so result fetch
#                      still fits under the local transport bound (RES-02). Bound via
#                      the process ladder: timeout(1) wrapper, else RuntimeMaxSec
#                      scope, else multi-hour ceiling (timeout 0), else refuse (exit 7).
#
# Config precedence: process env always wins over `.workbay/remote-gate.env`.
# TTL/idle values are snapshotted before the file is parsed and then resolved
# from literal KEY=VALUE assignments, so remote-gate.env may set
# DISPATCH_TTL_SEC, SANDBOX_TTL_SEC, or SANDBOX_IDLE_SEC without overriding an
# explicit process environment value. The file is never sourced.
# Shares the remote-gate host:
#   WORKBAY_REMOTE_GATE_HOST   required (e.g. gate@<host>); no baked-in default.
#   WORKBAY_REMOTE_GATE_DIR    remote clone dir (default src/<repo-slug>).
#   WORKBAY_REMOTE_AGENT_ROOT  sandbox parent dir (default workbay-sandbox;
#                      existing grok-sandbox resources remain discoverable).
#   WORKBAY_REMOTE_GATE_MEMORY_MAX / _CPU_QUOTA   per-run caps (6G / 200%).
#   WORKBAY_REMOTE_GATE_MEM_FLOOR_MB   VM MemAvailable floor (default 2048); the lane
#                      defers (exit 75) below this so non-lane work keeps its headroom.
#   WORKBAY_REMOTE_AGENT_MAX_LANES     concurrent workbay-lane-* scopes on the VM
#                      (optional, default none; when set, must be an integer >= 1); at/above the cap the
#                      lane defers (exit 75).
#   WORKBAY_REMOTE_GATE_PID_FLOOR      free pids the user slice must retain
#                      (default 64); systemd charges threads to pids.max and one
#                      lane cost is measured separately; 0 removes only the reserve.
#   WORKBAY_REMOTE_GATE_PIDS_ROOT      cgroup dir holding pids.current/pids.max
#                      (default /sys/fs/cgroup/user.slice/user-$(id -u).slice).
#   REMOTE_AGENT_ATTRIBUTION_DEADLINE_SEC  total scope-attribution budget
#                      (default 30 seconds; positive decimal, at most 300 seconds).
#   REMOTE_AGENT_ATTRIBUTION_CANDIDATE_CAP maximum scope directories read per
#                      attribution snapshot (default 128; valid range 1..4096).
#   WORKBAY_REMOTE_AGENT_MAX_LANE_VENVS  retained per-lane venvs (default 24);
#                      must stay > MAX_LANES so warm reuse is not LRU-evicted
#                      under a live lane. 0 disables reap.
#   WORKBAY_REMOTE_AGENT_DISPATCH_TTL_SEC  age TTL (seconds) for per-dispatch
#                      transient reaper (outbox/brief/schema/ref); default 86400
#                      (24h). 0 disables. Age is the live-dispatch safety guard.
#   WORKBAY_REMOTE_AGENT_SANDBOX_TTL_SEC  age TTL (seconds) for per-LANE sandbox
#                      dirs ($ROOT/<LANE_KEY>), their .venv-lane-* siblings, and
#                      orphan lane venvs (persisted venv with no matching sandbox);
#                      default 172800 (48h) so a sandbox kept for post-mortem
#                      survives a weekend-adjacent gap. 0 disables only this
#                      sweep (dispatch reaper still runs). Marker-gated sandboxes;
#                      orphan venvs are reclaimed when the sandbox is already gone.
#   WORKBAY_REMOTE_AGENT_KEEP_REFS  space-separated branch names the ref reaper
#                      must never delete (plus built-in main/master/HEAD). Escape
#                      hatch for real branches that end in 8 hex chars
#                      (e.g. hotfix-deadbeef) which the legacy lane-key shape
#                      cannot distinguish by name alone.
#   WORKBAY_REMOTE_AGENT_REAP_LEGACY_REFS  0|1; opt-in for reclaiming pre-nonce
#                      lane refs that end only in -<8hex> (default 0 = OFF).
#                      That shape is name-ambiguous with real branches such as
#                      release-20260726 / hotfix-deadbeef; KEEP_REFS is the
#                      operator escape hatch when this is set to 1. Nonce-tailed
#                      refs (-<8hex>-<pid>-<16hex>) are always eligible.
#   WORKBAY_REMOTE_AGENT_SWEEP_MIN_INTERVAL_SEC  successful sandbox-sweep
#                      recency window (default 60); 0 disables the skip.
#   WORKBAY_REMOTE_AGENT_SANDBOX_REAP_BUDGET_SEC  dispatch-local sandbox sweep
#                      budget (default 60); 0 still means a bounded minimum pass.
#
# Security: `git archive` ships TRACKED files only (no gitignored secrets) ->
# fresh `git init` -> ONE synthetic commit -> NO remote, so grok has no history,
# secrets, or remote to exfiltrate. GROK_ZDR_ENABLED=1 gates uploads too. The
# script asserts the sandbox is remote-severed before running grok.
#
# Exit: 0 patch produced · 3 grok run failed · 4 no committed changes ·
#       75 retryable defer (VM memory floor, lane cap, residual-timeout
#       exhausted pre-grok, or a same-branch lane already holding the lane lock)
#       · 7 = no process bound obtainable (policy refusal)
#       · 8 wall-clock bound expiry (deadline reached or timeout(1) status 124)
#       · 12 source ref missing · 13 source commit mismatch ·
#       14 source tree mismatch · 78 host not configured · 2 usage/validation error.
#
# Concurrency: lanes on DISTINCT branches run concurrently up to
# WORKBAY_REMOTE_AGENT_MAX_LANES. Lanes on the SAME branch share a LANE_KEY (it is
# derived from the branch name), hence one sandbox path — they are serialized by a
# non-blocking lane lock and the loser defers with exit 75. It is never correct for
# one lane to wipe another's live sandbox (internal).
set -euo pipefail

# Shared by explicit and dispatch sandbox reap; absent/oversized configs are no-ops.
_probe_trust_remote_sh=$(cat <<'PROBE_TRUST_SH'
_prune_probe_trust() {
    local _prune_rc=0
    if ! command -v timeout >/dev/null 2>&1; then
        echo "remote_agent: probe_trust_prune_skipped: timeout unavailable" >&2
        return 1
    fi
    timeout -k 1 10 python3 - "${CODEX_HOME:-$HOME/.codex}/config.toml" "$ROOT/.workbay-preflight-probes" <<'PROBE_TRUST_PY' || _prune_rc=$?
import copy
import os
from pathlib import Path
import re
import tempfile

try:
    import tomllib
    config = Path(__import__("sys").argv[1])
    slots = os.path.abspath(__import__("sys").argv[2])
    if config.is_symlink() or not config.is_file():
        raise SystemExit(0)
    before = config.stat()
    if before.st_size > 1024 * 1024:
        raise SystemExit(0)
    with config.open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise SystemExit(0)
    text = raw.decode("utf-8")
    parsed = tomllib.loads(text)
    expected = copy.deepcopy(parsed)
    # Only complete, standalone project trust tables. Preserve all other bytes.
    headers = list(re.finditer(r"(?m)^\s*\[[^\n]+\][ \t]*(?:#[^\n]*)?\n", text))
    pieces = []
    cursor = 0
    for index, header in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(text)
        block = text[header.start():end]
        try:
            table = tomllib.loads(block)
        except tomllib.TOMLDecodeError:
            continue
        projects = table.get("projects", {})
        if set(table) != {"projects"} or len(projects) != 1:
            continue
        path, entry = next(iter(projects.items()))
        if not isinstance(entry, dict) or set(entry) != {"trust_level"}:
            continue
        normalized = os.path.abspath(path)
        if not os.path.isabs(path) or not normalized.startswith(slots + os.sep):
            continue
        # lstat preserves dangling symlinks; any uncertainty preserves the entry.
        try:
            os.lstat(path)
            continue
        except FileNotFoundError:
            pass
        if expected.get("projects", {}).get(path) != entry:
            continue
        del expected["projects"][path]
        pieces.append(text[cursor:header.start()])
        cursor = end
    if not pieces:
        raise SystemExit(0)
    pieces.append(text[cursor:])
    updated = "".join(pieces)
    actual = tomllib.loads(updated)
    if not expected.get("projects"):
        expected.pop("projects", None)
    if actual != expected:
        raise SystemExit(0)
    # Atomic replacement, retaining permissions; abandon if a writer changed it.
    def config_identity(status):
        # Reading a config may update atime (notably on relatime filesystems).
        return (
            status.st_dev,
            status.st_ino,
            status.st_size,
            status.st_mode,
            status.st_uid,
            status.st_mtime_ns,
            status.st_ctime_ns,
        )

    fd, temporary = tempfile.mkstemp(prefix=".probe-trust-", dir=config.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), before.st_mode & 0o777)
            stream.write(updated.encode("utf-8"))
        if config_identity(config.stat()) == config_identity(before) and not config.is_symlink():
            os.replace(temporary, config)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
except (OSError, ValueError, ImportError):
    pass
PROBE_TRUST_PY
    if [ "$_prune_rc" -eq 124 ] || [ "$_prune_rc" -eq 137 ]; then
        echo "remote_agent: probe_trust_prune_timeout: status=$_prune_rc; prune skipped" >&2
        return 1
    elif [ "$_prune_rc" -ne 0 ]; then
        echo "remote_agent: probe_trust_prune_failed: status=$_prune_rc; prune skipped" >&2
        return 1
    fi
    return 0
}
PROBE_TRUST_SH
)


repo_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
repo_slug="$(basename "$repo_root")"

_review_cycle_bridge() {
    # Use this transport's package source when running from a checkout; an
    # installed transport resolves its installed runtime normally.
    local _src="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/packages"
    PYTHONPATH="${_src}/mcp-workbay-orchestrator/src:${_src}/mcp-workbay-handoff/src:${_src}/workbay-protocol/src${PYTHONPATH:+:$PYTHONPATH}" \
        python3 -c 'import sys; from workbay_orchestrator_mcp.orchestration.review_cycle_dispatch import shell_entry, shell_collect; sys.argv.pop(1) if sys.argv[1] == "collect" else None; shell_collect() if len(sys.argv) == 4 else shell_entry()' "$@"
}


# Read the consumer cadence from the unit that is installed with the queue.
# A missing or malformed timer leaves freshness unknown and therefore stale.
_queue_timer_file="$(dirname "${BASH_SOURCE[0]}")/vm/workbay-lane-queue.timer"
QUEUE_TICK_INTERVAL_S="$(timeout 5 sed -n 's/^OnUnitInactiveSec=\([0-9][0-9]*\)s$/\1/p' "$_queue_timer_file" 2>/dev/null || true)"
case "$QUEUE_TICK_INTERVAL_S" in ''|*[!0-9]*|0) QUEUE_TICK_INTERVAL_S="" ;; esac

# Literal KEY=VALUE reader for .workbay/remote-gate.env. Never source/execute
# the file (SECD-02). Optional export, single/double-quoted or bare literals,
# # comments, blank lines. No expansion, command substitution, or continuation.
_remote_gate_env_unquote() {
    local raw inner
    raw=$1
    case "$raw" in
        \'*\')
            inner=${raw#\'}
            inner=${inner%\'}
            case "$inner" in *\'*) return 1 ;; esac
            printf '%s' "$inner"
            return 0
            ;;
        \"*\")
            inner=${raw#\"}
            inner=${inner%\"}
            case "$inner" in
                *\$*|*\`*|*\\*) return 1 ;;
            esac
            printf '%s' "$inner"
            return 0
            ;;
        *)
            case "$raw" in
                *[!A-Za-z0-9@._:/-]*) return 1 ;;
            esac
            printf '%s' "$raw"
            return 0
            ;;
    esac
}

_remote_gate_env_load() {
    local file allow line trim key raw val
    file=$1
    shift
    allow=" $* "
    [ -f "$file" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%$'\r'}
        trim=$line
        while true; do
            case "$trim" in
                ' '*) trim=${trim# } ;;
                $'\t'*) trim=${trim#$'\t'} ;;
                *) break ;;
            esac
        done
        case "$trim" in
            ''|'#'*) continue ;;
        esac
        case "$trim" in
            export\ *|export$'\t'*)
                trim=${trim#export}
                while true; do
                    case "$trim" in
                        ' '*) trim=${trim# } ;;
                        $'\t'*) trim=${trim#$'\t'} ;;
                        *) break ;;
                    esac
                done
                ;;
        esac
        case "$trim" in
            *=*) ;;
            *) continue ;;
        esac
        key=${trim%%=*}
        raw=${trim#*=}
        case "$key" in
            [A-Za-z_]*) ;;
            *) continue ;;
        esac
        case "$key" in
            *[!A-Za-z0-9_]*) continue ;;
        esac
        case "$allow" in
            *" $key "*) ;;
            *) continue ;;
        esac
        val=$(_remote_gate_env_unquote "$raw") || continue
        printf -v "$key" '%s' "$val"
    done < "$file"
}

# Snapshot every WORKBAY_* knob BEFORE reading the config file so process env
# always wins (a file that exports WORKBAY_REMOTE_GATE_MEM_FLOOR_MB=0 must not
# silently zero the floor when the operator set a valid value in the env).
# Gate-file values are read only after these process snapshots are captured;
# HOST/DIR and the three documented TTL/idle knobs may fall back to that file.
_env_host="${WORKBAY_REMOTE_GATE_HOST:-}"
_env_dir="${WORKBAY_REMOTE_GATE_DIR:-}"
_env_agent_root_set=0
if [ "${WORKBAY_REMOTE_AGENT_ROOT+x}" = x ]; then
    _env_agent_root_set=1
    _env_agent_root="$WORKBAY_REMOTE_AGENT_ROOT"
else
    _env_agent_root=""
fi
_env_mem_max="${WORKBAY_REMOTE_GATE_MEMORY_MAX:-}"
_env_cpu_quota="${WORKBAY_REMOTE_GATE_CPU_QUOTA:-}"
_env_mem_floor="${WORKBAY_REMOTE_GATE_MEM_FLOOR_MB:-}"
_env_max_lanes="${WORKBAY_REMOTE_AGENT_MAX_LANES:-}"
_env_max_lane_venvs="${WORKBAY_REMOTE_AGENT_MAX_LANE_VENVS:-}"
_env_dispatch_ttl="${WORKBAY_REMOTE_AGENT_DISPATCH_TTL_SEC:-${DISPATCH_TTL_SEC:-}}"
_env_sandbox_ttl="${WORKBAY_REMOTE_AGENT_SANDBOX_TTL_SEC:-}"
_env_sandbox_ttl_legacy="${SANDBOX_TTL_SEC:-}"
_env_sandbox_idle="${WORKBAY_REMOTE_AGENT_SANDBOX_IDLE_SEC:-}"
_env_sandbox_idle_legacy="${SANDBOX_IDLE_SEC:-}"
_env_uv_cache_cap="${WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB:-}"
_env_keep_refs="${WORKBAY_REMOTE_AGENT_KEEP_REFS:-}"
_env_reap_legacy="${WORKBAY_REMOTE_AGENT_REAP_LEGACY_REFS:-}"
_env_sweep_min_interval="${WORKBAY_REMOTE_AGENT_SWEEP_MIN_INTERVAL_SEC:-${SWEEP_MIN_INTERVAL_SEC:-}}"
_env_sandbox_reap_budget="${WORKBAY_REMOTE_AGENT_SANDBOX_REAP_BUDGET_SEC:-${SANDBOX_REAP_BUDGET_SEC:-}}"
_env_unbounded_ceiling="${WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S:-}"
_env_probe_model="${WORKBAY_PREFLIGHT_PROBE_MODEL:-}"
_env_probe_timeout="${WORKBAY_PREFLIGHT_PROBE_TIMEOUT_S:-}"
_env_probe_max_live="${WORKBAY_PREFLIGHT_MAX_LIVE_PROBES:-}"
_env_probe_slot_wait="${WORKBAY_PREFLIGHT_VM_SLOT_WAIT_S:-}"
_env_probe_nonce="${WORKBAY_PROBE_NONCE:-}"
REMOTE_GATE_HOST="" REMOTE_GATE_DIR=""
config_file="$repo_root/.workbay/remote-gate.env"
_remote_gate_env_load "$config_file" \
    REMOTE_GATE_HOST REMOTE_GATE_DIR \
    DISPATCH_TTL_SEC SANDBOX_TTL_SEC SANDBOX_IDLE_SEC \
    WORKBAY_REMOTE_AGENT_DISPATCH_TTL_SEC \
    WORKBAY_REMOTE_AGENT_SANDBOX_TTL_SEC \
    WORKBAY_REMOTE_AGENT_SANDBOX_IDLE_SEC \
    WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB \
    WORKBAY_REMOTE_AGENT_SWEEP_MIN_INTERVAL_SEC SWEEP_MIN_INTERVAL_SEC \
    WORKBAY_REMOTE_AGENT_SANDBOX_REAP_BUDGET_SEC SANDBOX_REAP_BUDGET_SEC

# BEGIN ADAPTER_HOST_GUARD
REMOTE_HOST="${_env_host:-${REMOTE_GATE_HOST:-}}"
if [ -z "$REMOTE_HOST" ]; then
    if [ "${1:-}" = "writable-roots-probe" ] || [ "${1:-}" = "workspace-write-probe" ]; then
        printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":false,"verdict":"unknown","reason":"transport_failure","elapsed_s":0,"live_probe_count":0,"head":"","model":"%s","nonce":"%s","sandbox_flags":"workspace-write"}\n' \
            "${1}" "${WORKBAY_PREFLIGHT_PROBE_MODEL:-gpt-5.6-sol}" "${WORKBAY_PROBE_NONCE:-}"
        exit 0
    fi
    echo "remote_agent: host not configured — set WORKBAY_REMOTE_GATE_HOST or" \
         "REMOTE_GATE_HOST in .workbay/remote-gate.env (e.g. gate@<your-host>)" >&2
    exit 78
fi
# END ADAPTER_HOST_GUARD
REMOTE_DIR="${_env_dir:-${REMOTE_GATE_DIR:-src/${repo_slug}}}"
# Process snapshots win. TTL/idle fall back to their post-parse values so the
# gate file can tune reclamation without affecting unrelated admission knobs.
if [ "$_env_agent_root_set" -eq 1 ]; then
    AGENT_ROOT="$_env_agent_root"
else
    AGENT_ROOT="workbay-sandbox"
fi
MEM_MAX="${_env_mem_max:-6G}"
CPU_QUOTA="${_env_cpu_quota:-200%}"
# Arm-3 ceiling for --timeout 0 (seconds): injectable so V3/V6 can shrink it;
# default 21600 matches the pre-ladder 6h lease ceiling [RES-02][FM-08].
# Read from the pre-parse env snapshot only — never re-read WORKBAY_* after
# parsing remote-gate.env ([REF-10] process env wins over file).
WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S="${_env_unbounded_ceiling:-21600}"
PREFLIGHT_PROBE_MODEL="${_env_probe_model:-gpt-5.6-sol}"
PREFLIGHT_PROBE_TIMEOUT_S="${_env_probe_timeout:-180}"
PREFLIGHT_PROBE_MAX_LIVE="${_env_probe_max_live:-2}"
PREFLIGHT_PROBE_SLOT_WAIT_S="${_env_probe_slot_wait:-60}"
PREFLIGHT_PROBE_NONCE="${_env_probe_nonce:-workbay-probe-$$}"
# VM MemAvailable floor (MiB): defer the lane when the VM is below this, reserving
# headroom for ALL non-lane work on the box (co-resident mission-critical procs).
MEM_FLOOR_MB="${_env_mem_floor:-2048}"
# Optional operator ceiling; unset/empty means no count ceiling. Resource gauges
# determine admission independently of the number of lanes.
MAX_LANES="${_env_max_lanes:-}"
# Additional pid reserve after paying the measured per-lane cost.
PID_FLOOR="${WORKBAY_REMOTE_GATE_PID_FLOOR:-64}"
HEADROOM_PCT="${WORKBAY_REMOTE_GATE_HEADROOM_PCT:-75}"
MEM_PSI_MAX="${WORKBAY_REMOTE_GATE_MEM_PSI_MAX:-10}"
CPU_PSI_MAX="${WORKBAY_REMOTE_GATE_CPU_PSI_MAX:-60}"
DISK_FLOOR_GB="${WORKBAY_REMOTE_GATE_DISK_FLOOR_GB:-10}"
LANE_PIDS_FLOOR="${WORKBAY_REMOTE_GATE_LANE_PIDS_FLOOR:-66}"
LANE_ANON_FLOOR_MB="${WORKBAY_REMOTE_GATE_LANE_ANON_FLOOR_MB:-512}"
# Retention cap for PERSISTED per-lane venvs (internal S4):
# venvs survive the per-pass wipe by design, so they accumulate one per distinct
# offloaded branch and would grow the VM disk unbounded. Keep the N most-recently-
# used; LRU-evict the rest (with their sync stamps) at build time. 0 = keep all
# (disable reap). Default 24 retains warm venvs across normal rotation --
# a cap at or below MAX_LANES would LRU-evict a venv still owned by a live lane and
# force every rotation to cold-sync.
LANE_VENV_CAP="${_env_max_lane_venvs:-24}"
# Age TTL (seconds) for PER-DISPATCH transients (outbox/brief/schema/ref) [RES-07]:
# these grow one set per DISPATCH, not per branch — a count cap is wrong-shaped.
# EXIT trap is the fast path; this reaper is the backstop when the trap does not
# run (kill / ssh failure / VM death). Age is the live-dispatch guard: only entries
# older than TTL are removed. 0 = disable. Default 86400 (24h) >> longest turn.
_file_dispatch_ttl="${WORKBAY_REMOTE_AGENT_DISPATCH_TTL_SEC:-${DISPATCH_TTL_SEC:-86400}}"
DISPATCH_TTL_SEC="${_env_dispatch_ttl:-86400}"
if [ -z "$_env_dispatch_ttl" ]; then
    DISPATCH_TTL_SEC="$_file_dispatch_ttl"
fi
# Age TTL (seconds) for PER-LANE sandboxes ($ROOT/<LANE_KEY>) [RES-07]: these leak
# forever when a lane key is never re-dispatched (same-key wipe is the only prior
# reclaim path). Default 172800 (48h) so a post-mortem sandbox survives a
# weekend-adjacent gap; must stay >= 24h so a paused/deferred lane is not reaped
# while still wanted. 0 = disable only this sweep (dispatch reaper independent).
_file_sandbox_ttl="${WORKBAY_REMOTE_AGENT_SANDBOX_TTL_SEC:-${SANDBOX_TTL_SEC:-172800}}"
SANDBOX_TTL_SEC="${_env_sandbox_ttl:-172800}"
if [ -z "$_env_sandbox_ttl" ]; then
    SANDBOX_TTL_SEC="${_env_sandbox_ttl_legacy:-$_file_sandbox_ttl}"
fi
# Standalone sweep idle guard. Liveness (lock/lease/process cwd) always wins;
# this clock is secondary to the orchestrator-supplied ownership allowlist.
_file_sandbox_idle="${WORKBAY_REMOTE_AGENT_SANDBOX_IDLE_SEC:-${SANDBOX_IDLE_SEC:-21600}}"
SANDBOX_IDLE_SEC="${_env_sandbox_idle:-${_env_sandbox_idle_legacy:-$_file_sandbox_idle}}"
# Successful dispatch sandbox sweeps are recency-stamped in the durable
# .reap-last.json receipt. A pressured VM bypasses this skip so each dispatch
# remains a reclaim opportunity. Process environment wins over the gate file.
_file_sweep_min_interval="${WORKBAY_REMOTE_AGENT_SWEEP_MIN_INTERVAL_SEC:-${SWEEP_MIN_INTERVAL_SEC:-60}}"
SWEEP_MIN_INTERVAL_SEC="${_env_sweep_min_interval:-60}"
if [ -z "$_env_sweep_min_interval" ]; then
    SWEEP_MIN_INTERVAL_SEC="$_file_sweep_min_interval"
fi
# Dispatch-local sandbox sweep budget. This is deliberately separate from
# REAP_BUDGET_SEC, which bounds only the explicit standalone `reap` command.
_file_sandbox_reap_budget="${WORKBAY_REMOTE_AGENT_SANDBOX_REAP_BUDGET_SEC:-${SANDBOX_REAP_BUDGET_SEC:-60}}"
SANDBOX_REAP_BUDGET_SEC="${_env_sandbox_reap_budget:-60}"
if [ -z "$_env_sandbox_reap_budget" ]; then
    SANDBOX_REAP_BUDGET_SEC="$_file_sandbox_reap_budget"
fi
# Host-side uv cache budget. Process environment wins over the gate file;
# 0 disables pruning. Prune removes unused entries only (never cache clean).
_file_uv_cache_cap="${WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB:-4096}"
UV_CACHE_CAP_MB="${_env_uv_cache_cap:-4096}"
if [ -z "$_env_uv_cache_cap" ]; then
    UV_CACHE_CAP_MB="$_file_uv_cache_cap"
fi
# Space-separated branch names the ref reaper must preserve in addition to the
# built-in set (main master HEAD). Needed because a real branch ending in 8 hex
# chars (hotfix-deadbeef) is indistinguishable from a legacy lane-key ref by
# shape alone. Empty = no extras. Charset-restricted: interpolated into the
# remote body, so quotes/$/backticks/semicolons are refused at validation.
KEEP_REFS="${_env_keep_refs:-}"
# Opt-in legacy ref sweep (0|1, default 0). Pre-nonce lane refs are bare
# LANE_KEY (-<8hex> only); that tail is not unique to lane refs, so reclaim
# requires explicit operator opt-in. Nonce-tailed refs stay always-on.
REAP_LEGACY_REFS="${_env_reap_legacy:-0}"

# validation (interpolated into the remote shell)
_validate_attribution_options() {
    local _vao_whole _vao_fraction _vao_ms
    ATTRIBUTION_DEADLINE_SEC="${REMOTE_AGENT_ATTRIBUTION_DEADLINE_SEC-30}"
    ATTRIBUTION_CANDIDATE_CAP="${REMOTE_AGENT_ATTRIBUTION_CANDIDATE_CAP-128}"
    if [[ ! "$ATTRIBUTION_DEADLINE_SEC" =~ ^([0-9]{1,3})([.]([0-9]{1,3}))?$ ]]; then
        printf 'remote_agent: attribution_option_invalid name=REMOTE_AGENT_ATTRIBUTION_DEADLINE_SEC value=%s\n' "$ATTRIBUTION_DEADLINE_SEC" >&2
        exit 2
    fi
    _vao_whole=${BASH_REMATCH[1]}
    _vao_fraction=${BASH_REMATCH[3]:-000}
    while [ "${#_vao_fraction}" -lt 3 ]; do _vao_fraction="${_vao_fraction}0"; done
    _vao_ms=$((10#$_vao_whole * 1000 + 10#$_vao_fraction))
    if [ "$_vao_ms" -le 0 ] || [ "$_vao_ms" -gt 300000 ]; then
        printf 'remote_agent: attribution_option_invalid name=REMOTE_AGENT_ATTRIBUTION_DEADLINE_SEC value=%s\n' "$ATTRIBUTION_DEADLINE_SEC" >&2
        exit 2
    fi
    if [[ ! "$ATTRIBUTION_CANDIDATE_CAP" =~ ^[0-9]+$ ]] || [ "${#ATTRIBUTION_CANDIDATE_CAP}" -gt 5 ]; then
        printf 'remote_agent: attribution_option_invalid name=REMOTE_AGENT_ATTRIBUTION_CANDIDATE_CAP value=%s\n' "$ATTRIBUTION_CANDIDATE_CAP" >&2
        exit 2
    fi
    if [ "$((10#$ATTRIBUTION_CANDIDATE_CAP))" -lt 1 ] || [ "$((10#$ATTRIBUTION_CANDIDATE_CAP))" -gt 4096 ]; then
        printf 'remote_agent: attribution_option_invalid name=REMOTE_AGENT_ATTRIBUTION_CANDIDATE_CAP value=%s\n' "$ATTRIBUTION_CANDIDATE_CAP" >&2
        exit 2
    fi
}
_validate_attribution_options
case "$REMOTE_DIR" in ""|.|/*|*..*|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid REMOTE_DIR" >&2; exit 2 ;; esac
# Only status transports this value as an encoded assignment in Bash stdin.
# Validate its literal ASCII spaces separately; every other mode stays strict.
_agent_root_check="$AGENT_ROOT"
if [ "${1:-}" = status ]; then
    _agent_root_check="${_agent_root_check// /}"
fi
case "$_agent_root_check" in ""|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid AGENT_ROOT" >&2; exit 2 ;; esac
case "/$AGENT_ROOT/" in *"/../"*) echo "remote_agent: invalid AGENT_ROOT" >&2; exit 2 ;; esac
case "$MEM_MAX" in *[!0-9GMK]*|"") echo "remote_agent: MEMORY_MAX must look like 6G/512M" >&2; exit 2 ;; esac
case "$CPU_QUOTA" in *[!0-9%]*|"") echo "remote_agent: CPU_QUOTA must look like 200%" >&2; exit 2 ;; esac
case "$MEM_FLOOR_MB" in *[!0-9]*|"") echo "remote_agent: MEM_FLOOR_MB must be an integer (MiB)" >&2; exit 2 ;; esac
case "$MAX_LANES" in *[!0-9]*) echo "remote_agent: MAX_LANES must be an integer >= 1" >&2; exit 2 ;; esac
case "$PID_FLOOR" in *[!0-9]*|"") echo "remote_agent: PID_FLOOR must be a non-negative integer (0=no extra pid reserve)" >&2; exit 2 ;; esac
for _gauge_value in "$HEADROOM_PCT" "$MEM_PSI_MAX" "$CPU_PSI_MAX" "$DISK_FLOOR_GB" "$LANE_PIDS_FLOOR" "$LANE_ANON_FLOOR_MB"; do
    case "$_gauge_value" in *[!0-9]*|"") echo "remote_agent: measured admission thresholds must be non-negative integers" >&2; exit 2 ;; esac
done
if [ "$HEADROOM_PCT" -lt 1 ] || [ "$HEADROOM_PCT" -gt 100 ]; then
    echo "remote_agent: HEADROOM_PCT must be between 1 and 100" >&2
    exit 2
fi
case "$LANE_VENV_CAP" in *[!0-9]*|"") echo "remote_agent: MAX_LANE_VENVS must be a non-negative integer (0=keep all)" >&2; exit 2 ;; esac
case "$DISPATCH_TTL_SEC" in *[!0-9]*|"") echo "remote_agent: DISPATCH_TTL_SEC must be a non-negative integer (0=disable)" >&2; exit 2 ;; esac
case "$SANDBOX_TTL_SEC" in *[!0-9]*|"") echo "remote_agent: SANDBOX_TTL_SEC must be a non-negative integer (0=disable)" >&2; exit 2 ;; esac
case "$SANDBOX_IDLE_SEC" in *[!0-9]*|"") echo "remote_agent: SANDBOX_IDLE_SEC must be a non-negative integer" >&2; exit 2 ;; esac
case "$SWEEP_MIN_INTERVAL_SEC" in *[!0-9]*|"") echo "remote_agent: SWEEP_MIN_INTERVAL_SEC must be a non-negative integer" >&2; exit 2 ;; esac
case "$SANDBOX_REAP_BUDGET_SEC" in *[!0-9]*|"") echo "remote_agent: SANDBOX_REAP_BUDGET_SEC must be a non-negative integer" >&2; exit 2 ;; esac
case "$UV_CACHE_CAP_MB" in *[!0-9]*|"") echo "remote_agent: UV_CACHE_CAP_MB must be a non-negative integer" >&2; exit 2 ;; esac
case "$WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S" in *[!0-9]*|"") echo "remote_agent: WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S must be an integer >= 1 (0 cannot bound anything; refuse at admission, not exit 75)" >&2; exit 2 ;; esac
case "$PREFLIGHT_PROBE_MODEL" in ""|*[!A-Za-z0-9._:/-]*) echo "remote_agent: WORKBAY_PREFLIGHT_PROBE_MODEL contains unsupported characters" >&2; exit 2 ;; esac
case "$PREFLIGHT_PROBE_TIMEOUT_S" in *[!0-9]*|"") echo "remote_agent: WORKBAY_PREFLIGHT_PROBE_TIMEOUT_S must be a positive integer" >&2; exit 2 ;; esac
case "$PREFLIGHT_PROBE_MAX_LIVE" in *[!0-9]*|"") echo "remote_agent: WORKBAY_PREFLIGHT_MAX_LIVE_PROBES must be a non-negative integer" >&2; exit 2 ;; esac
case "$PREFLIGHT_PROBE_SLOT_WAIT_S" in *[!0-9]*|"") echo "remote_agent: WORKBAY_PREFLIGHT_VM_SLOT_WAIT_S must be a non-negative integer" >&2; exit 2 ;; esac
case "$PREFLIGHT_PROBE_NONCE" in ""|*[!A-Za-z0-9._:-]*) echo "remote_agent: WORKBAY_PROBE_NONCE contains unsupported characters" >&2; exit 2 ;; esac
case "$KEEP_REFS" in *[!A-Za-z0-9/_.\ -]*) echo "remote_agent: KEEP_REFS may only contain letters, digits, / _ . - and spaces" >&2; exit 2 ;; esac
case "$REAP_LEGACY_REFS" in 0|1) ;; *) echo "remote_agent: REAP_LEGACY_REFS must be 0 or 1" >&2; exit 2 ;; esac
if [ -n "$MAX_LANES" ] && [ "$MAX_LANES" -lt 1 ]; then
    echo "remote_agent: MAX_LANES must be an integer >= 1" >&2
    exit 2
fi
# Preserve warm retention above an explicit operator ceiling; 0 keeps all venvs.
if [ -n "$MAX_LANES" ] && [ "$LANE_VENV_CAP" -gt 0 ] && [ "$LANE_VENV_CAP" -le "$MAX_LANES" ]; then
    LANE_VENV_CAP=$(( MAX_LANES + 1 ))
fi
if [ "$WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S" -lt 1 ]; then
    echo "remote_agent: WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S must be an integer >= 1 (0 cannot bound anything; refuse at admission, not exit 75)" >&2
    exit 2
fi

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=4 "$REMOTE_HOST")
die() { echo "remote_agent: $*" >&2; exit 2; }

# BEGIN REMOTE_SUBMIT_JOB
_remote_submit_job() {
    set -euo pipefail
    : "${JOB_ID:?}" "${LANE_KEY:?}" "${AGENT_ROOT:?}"
    : "${MEM_MAX:?}" "${CPU_QUOTA:?}" "${JOB_RUNTIME_SEC:?}"
    : "${UV_CACHE_CAP_MB:?}" "${DISPATCH_NONCE:?}" "${BODY_B64:?}"
    case "$AGENT_ROOT" in /*) ROOT="$AGENT_ROOT" ;; *) ROOT="$HOME/$AGENT_ROOT" ;; esac
    JOB_DIR="$ROOT/.jobs/${JOB_ID}"
    UNIT="workbay-lane-${LANE_KEY}-job-${JOB_ID}.service"

    _submit_write_state() {
        _st="$1"
        printf '{"state":"%s","unit":"%s"}\n' "$_st" "$UNIT" > "$JOB_DIR/state.json.tmp"
        mv -f "$JOB_DIR/state.json.tmp" "$JOB_DIR/state.json"
    }
    _submit_print() {
        printf '{"job_id":"%s","state":"%s","unit":"%s"}\n' "$JOB_ID" "$1" "$UNIT"
    }
    _submit_crash_if() {
        if [ "${WORKBAY_REMOTE_SUBMIT_CRASH_AFTER:-}" = "$1" ]; then
            echo "remote_agent: submit crash after $1" >&2
            exit 99
        fi
    }
    _submit_unit_active() {
        if timeout 10 systemctl --user is-active "$UNIT" >/dev/null 2>&1; then
            return 0
        fi
        return 1
    }
    _submit_require_linger() {
        _linger="$(timeout 10 loginctl show-user "${USER}" -p Linger --value 2>/dev/null || true)"
        if [ "$_linger" != "yes" ]; then
            echo "remote_agent: vm_linger_disabled" >&2
            exit 78
        fi
    }
    _submit_write_body_and_env() {
        if ! printf '%s\n' "$BODY_B64" | base64 -d > "$JOB_DIR/body.sh.tmp" 2>/dev/null; then
            printf '%s\n' "$BODY_B64" | base64 --decode > "$JOB_DIR/body.sh.tmp"
        fi
        mv -f "$JOB_DIR/body.sh.tmp" "$JOB_DIR/body.sh"
        chmod 0600 "$JOB_DIR/body.sh"
        BODY_B64=""
        {
            printf 'WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB=%s\n' "$UV_CACHE_CAP_MB"
            printf 'WORKBAY_DISPATCH_NONCE=%s\n' "$DISPATCH_NONCE"
            printf 'WORKBAY_SUBMIT_REMOTE_GIT=%s\n' "${REMOTE_GIT:-}"
            printf 'WORKBAY_SUBMIT_REF_NAME=%s\n' "${REF_NAME:-}"
            printf 'WORKBAY_SUBMIT_BRIEF=%s\n' "${BRIEF_PATH:-}"
            printf 'WORKBAY_SUBMIT_OUTBOX=%s\n' "${OUTBOX_PATH:-}"
        } > "$JOB_DIR/env.tmp"
        mv -f "$JOB_DIR/env.tmp" "$JOB_DIR/env"
        chmod 0600 "$JOB_DIR/env"
        printf '%s\n' "${BRIEF_PATH:-}" "${OUTBOX_PATH:-}" "${REF_NAME:-}" > "$JOB_DIR/transients.tmp"
        mv -f "$JOB_DIR/transients.tmp" "$JOB_DIR/transients"
        cat > "$JOB_DIR/wrapper.sh" <<'WRAP'
#!/bin/bash
set -u
touch started
cd "$(dirname "$0")"
export WORKBAY_REMOTE_JOB_DIR="$PWD"
_hb=""
_finished=0
_heartbeat() {
    while true; do
        touch heartbeat
        sleep 30
    done
}
_finish_job() {
    _rc="${1:-1}"
    [ "${_finished}" = 1 ] && return 0
    _finished=1
    if [ -n "${_hb}" ]; then
        kill "${_hb}" 2>/dev/null || true
        wait "${_hb}" 2>/dev/null || true
    fi
    _job_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)" || return 1
    _finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    _stdout_bytes="$(wc -c < "${_job_dir}/stdout" 2>/dev/null | tr -d ' ')"
    case "${_stdout_bytes}" in ''|*[!0-9]*) _stdout_bytes=0 ;; esac
    _stdout_sha256="$(sha256sum "${_job_dir}/stdout" 2>/dev/null | awk '{print $1}')"
    [ -n "${_stdout_sha256}" ] || _stdout_sha256="missing"
    _job_id="$(basename -- "${_job_dir}")"
    exec 9>>"${_job_dir}/.lock"
    flock -w 30 9 || true
    if [ -f "${_job_dir}/done.json" ]; then
        return 0
    fi
    if [ -f "${_job_dir}/cancel.fence" ]; then
        return 0
    fi
    _artifact_snapshot_failed=0
    # Snapshot only known dispatch artifacts before publishing completion/cleanup.
    if ! WORKBAY_JOB_DIR="${_job_dir}" python3 - <<'PYART'
import hashlib
import json
import os
import stat
from pathlib import Path

job = Path(os.environ["WORKBAY_JOB_DIR"])
artifacts = job / "artifacts"
artifacts.mkdir(exist_ok=True)
manifest = {}
outbox = os.environ.get("WORKBAY_SUBMIT_OUTBOX", "")
outbox_path = Path(os.path.abspath(outbox)) if outbox else None
for key, names in {
    "result-out": (".agent-result.json", ".grok-result.json"),
    "debug-out": ".grok-debug.log",
    "stream-out": ".agent-stream.jsonl",
    "selfverify-out": ".grok-selfverify.json",
    "uncommitted-out": "uncommitted-remainder.patch",
    "phases-out": (".agent-phases.json", ".grok-phases.json"),
    "provenance-out": ".workbay-source-provenance.json",
}.items():
    if isinstance(names, str):
        names = (names,)
    candidates = (outbox_path / name for name in names) if outbox_path else ()
    source = next((candidate for candidate in candidates if candidate.exists() or candidate.is_symlink()), None)
    if source is None:
        continue
    if source.is_symlink():
        outbox_name = outbox_path.name if outbox_path else ""
        prefix = ".lane-out-"
        suffix = outbox_name[len(prefix) :] if outbox_name.startswith(prefix) else ""
        if not outbox_path or not suffix:
            raise ValueError("invalid artifact source")
        evidence = outbox_path.parent / f"attempt-evidence-{suffix}"
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        parent_fd = os.open(outbox_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            outbox_fd = os.open(outbox_path.name, directory_flags, dir_fd=parent_fd)
            try:
                evidence_fd = os.open(evidence.name, directory_flags, dir_fd=parent_fd)
                try:
                    if not stat.S_ISDIR(os.fstat(outbox_fd).st_mode) or not stat.S_ISDIR(
                        os.fstat(evidence_fd).st_mode
                    ):
                        raise ValueError("invalid artifact source")
                    link_target = Path(os.readlink(source.name, dir_fd=outbox_fd))
                    if not link_target.is_absolute():
                        link_target = Path(os.path.abspath(outbox_path / link_target))
                    target = evidence / source.name
                    if link_target != target:
                        raise ValueError("invalid artifact source")
                    target_stat = os.stat(source.name, dir_fd=evidence_fd, follow_symlinks=False)
                    if not stat.S_ISREG(target_stat.st_mode):
                        raise ValueError("invalid artifact source")
                    fd = os.open(
                        source.name,
                        os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                        dir_fd=evidence_fd,
                    )
                    with os.fdopen(fd, "rb") as stream:
                        opened_stat = os.fstat(stream.fileno())
                        if not stat.S_ISREG(opened_stat.st_mode) or (
                            opened_stat.st_dev,
                            opened_stat.st_ino,
                        ) != (target_stat.st_dev, target_stat.st_ino):
                            raise ValueError("invalid artifact source")
                        data = stream.read()
                finally:
                    os.close(evidence_fd)
            finally:
                os.close(outbox_fd)
        finally:
            os.close(parent_fd)
    else:
        if not source.is_file() or (outbox_path and outbox_path.is_symlink()):
            raise ValueError("invalid artifact source")
        data = source.read_bytes()
    target = artifacts / key
    with target.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    manifest[key] = hashlib.sha256(data).hexdigest()
with (job / "artifacts.json").open("w") as stream:
    json.dump({"job_id": job.name, "files": manifest}, stream)
    stream.flush()
    os.fsync(stream.fileno())
PYART
    then
        echo "remote_agent: artifact_snapshot_failed" >&2
        _artifact_snapshot_failed=1
        printf '{"error":"artifact_snapshot_failed"}\n' > "${_job_dir}/artifacts.json"
    fi
    printf '{"finished_at":"%s","job_id":"%s","rc":%s,"reason":null,"schema_version":1,"stdout_bytes":%s,"stdout_sha256":"%s"}\n' \
        "${_finished_at}" "${_job_id}" "${_rc}" "${_stdout_bytes}" "${_stdout_sha256}" > "${_job_dir}/done.json.tmp"
    if ln "${_job_dir}/done.json.tmp" "${_job_dir}/done.json" 2>/dev/null; then
        rm -f "${_job_dir}/done.json.tmp"
    else
        rm -f "${_job_dir}/done.json.tmp"
        return 0
    fi
    if [ -f "${_job_dir}/transients" ]; then
        _brief="$(sed -n '1p' "${_job_dir}/transients")"
        _outbox="$(sed -n '2p' "${_job_dir}/transients")"
        _ref="$(sed -n '3p' "${_job_dir}/transients")"
        [ -n "${_brief}" ] && rm -f "${_brief}"
        [ "${_artifact_snapshot_failed}" = 0 ] && [ -n "${_outbox}" ] && rm -rf "${_outbox}"
        if [ -n "${WORKBAY_SUBMIT_REMOTE_GIT:-}" ] && [ -n "${_ref}" ]; then
            git -C "${WORKBAY_SUBMIT_REMOTE_GIT}" update-ref -d "${_ref}" 2>/dev/null || true
        fi
    fi
}
trap '_finish_job $?' EXIT
_heartbeat &
_hb=$!
set -a
. ./env
set +a
bash ./body.sh >stdout 2>stderr
_body_rc=$?
_finish_job "${_body_rc}"
trap - EXIT
exit "${_body_rc}"
WRAP
        chmod 0700 "$JOB_DIR/wrapper.sh"
    }
    _submit_queue_fresh() {
        # Installed configuration and native heartbeat are read-only evidence.
        # Candidate timer/source bytes cannot attest or promote shared runtime.
        timeout 10 systemctl --user is-active workbay-lane-queue.timer >/dev/null 2>&1 || return 1
        timeout 10 python3 - "$ROOT" "$HOME" <<'QUEUEPY'
import json
import os
import re
import shlex
import sys
import time
from datetime import datetime
from pathlib import Path

root, home = map(Path, sys.argv[1:])
units = Path(os.environ.get("XDG_CONFIG_HOME") or home / ".config") / "systemd/user"
try:
    if not units.is_absolute():
        raise ValueError("queue config root unknown")
    paths = [home / ".local/lib/workbay/lane_queue.py", root / ".queue-state.json"]
    paths += [units / f"workbay-lane-queue.{suffix}" for suffix in ("service", "path", "timer")]
    if any(p.is_symlink() or not p.is_file() for p in paths):
        raise ValueError("queue installation unavailable")
    def directives(path, section, name):
        current = None
        values = []
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith(("#", ";")):
                continue
            if line.endswith(chr(92)):
                raise ValueError("queue continuation unknown")
            if line.startswith("[") and line.endswith("]"):
                current = line
            elif current == section and line.startswith(name + "="):
                values.append(line.split("=", 1)[1])
        return values

    def effective_path(value, *, relative=False):
        # Match the installed consumer's HOME-relative root and systemd's %h.
        value = value.replace("%h", str(home))
        if not value or "%" in value or "$" in value or "~" in value:
            raise ValueError("queue path expansion unknown")
        path = Path(value)
        if not path.is_absolute():
            if not relative:
                raise ValueError("queue path is relative")
            path = home / path
        return path.resolve(strict=True)

    # Overrides cannot be attested from the stock unit's text alone.
    for suffix in ("service", "path", "timer"):
        if (units / f"workbay-lane-queue.{suffix}.d").exists():
            raise ValueError("queue override unknown")
    environments = directives(paths[2], "[Service]", "Environment")
    settings = [item.split("=", 1)[1] for value in environments for item in shlex.split(value)
                if item.startswith("WORKBAY_REMOTE_AGENT_ROOT=")]
    if len(settings) != 1 or any(not value for value in environments):
        raise ValueError("queue root unknown")
    if directives(paths[2], "[Service]", "EnvironmentFile") or directives(paths[2], "[Service]", "UnsetEnvironment"):
        raise ValueError("queue environment override unknown")
    if effective_path(settings[0], relative=True) != root.resolve(strict=True):
        raise ValueError("queue root mismatch")
    for name in ("PathChanged", "PathModified"):
        values = directives(paths[3], "[Path]", name)
        if len(values) != 1 or effective_path(values[0]) != (root / ".jobs").resolve(strict=True):
            raise ValueError("queue publication path mismatch")
    cadence = re.search(r"^OnUnitInactiveSec=([0-9]+)s$", paths[4].read_text(), re.MULTILINE)
    if cadence is None or int(cadence[1]) <= 0:
        raise ValueError("queue cadence unavailable")
    limit = 2 * int(cadence[1]) + 15
    state = json.loads(paths[1].read_text())
    at = datetime.fromisoformat(state["at"].replace("Z", "+00:00"))
    if at.tzinfo is None or not isinstance(state["measure"], dict):
        raise ValueError("queue heartbeat unknown")
    if any(type(state[k]) is not int or state[k] < 0 for k in ("queued", "running")):
        raise ValueError("queue heartbeat unknown")
    now = time.time()
    if any(not 0 <= now - timestamp <= limit for timestamp in (at.timestamp(), paths[1].stat().st_mtime)):
        raise ValueError("queue heartbeat stale")
except (OSError, ValueError, KeyError, TypeError, AttributeError):
    sys.exit(1)
QUEUEPY
    }
    _submit_write_start() {
        # Only the installed queue consumer invokes this bounded launcher.
        {
            printf '#!/bin/bash\nset -euo pipefail\n'
            printf 'UNIT=%q\nJOB_DIR=%q\n' "$UNIT" "$JOB_DIR"
            printf 'JOB_RUNTIME_SEC=%q\nMEM_MAX=%q\nCPU_QUOTA=%q\n' "$JOB_RUNTIME_SEC" "$MEM_MAX" "$CPU_QUOTA"
            printf 'ADMISSION_B64=%q\n' "${ADMISSION_B64:-}"
            cat <<'START'
mkdir -p "$HOME/.local/lib/workbay"
exec 8>"$HOME/.local/lib/workbay/.admission.lock"
flock -w 30 8 || { echo 'remote_agent: vm_admission_lock_timeout' >&2; exit 75; }
if [ -n "$ADMISSION_B64" ]; then
    printf '%s' "$ADMISSION_B64" | base64 -d | bash
fi
# Keep the check and start under one fence (CON-11).
timeout 30 systemd-run --quiet --user --unit "$UNIT" --service-type=exec \
    -p "RuntimeMaxSec=${JOB_RUNTIME_SEC}" \
    -p "MemoryMax=${MEM_MAX}" -p "CPUQuota=${CPU_QUOTA}" \
    -p "ExecStopPost=/bin/bash $JOB_DIR/stop.sh" \
    --working-directory="$JOB_DIR" /bin/bash "$JOB_DIR/wrapper.sh" 7>&- 8>&- 9>&-
START
        } > "$JOB_DIR/start.sh.tmp"
        chmod 0700 "$JOB_DIR/start.sh.tmp"
        mv -f "$JOB_DIR/start.sh.tmp" "$JOB_DIR/start.sh"
        cat > "$JOB_DIR/stop.sh" <<'STOP'
#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
[ ! -f done.json ] || exit 0
if [ -f owned-scopes ]; then
    # Cancel fences under this lock; recheck and publish while holding it.
    # Without the lock, cancel or the wrapper finisher publishes instead.
    exec 9>>.lock || exit 0
    flock -w 30 9 || exit 0
    [ ! -f done.json ] || exit 0
fi
[ ! -f cancel.fence ] || exit 0
_rc="${EXIT_STATUS:-1}"
case "${EXIT_CODE:-exited}" in
    killed|dumped)
        case "$_rc" in
            *[!0-9]*) _rc="$(kill -l "$_rc" 2>/dev/null || echo 1)" ;;
        esac
        case "$_rc" in ''|*[!0-9]*) _rc=1 ;; esac
        _rc=$((128 + _rc)) ;;
esac
case "$_rc" in ''|*[!0-9]*) _rc=1 ;; esac
[ "${SERVICE_RESULT:-success}" = success ] || [ "$_rc" != 0 ] || _rc=1
# Publish without replacing the wrapper's terminal event.
WORKBAY_STOP_RC="$_rc" python3 - <<'STOPPY'
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
job = Path.cwd()
stdout = (job / "stdout").read_bytes() if (job / "stdout").exists() else b""
record = {"schema_version": 1, "job_id": job.name,
          "finished_at": datetime.now(timezone.utc).isoformat(),
          "rc": int(os.environ["WORKBAY_STOP_RC"]),
          "reason": os.environ.get("SERVICE_RESULT", "unknown"),
          "stdout_bytes": len(stdout), "stdout_sha256": hashlib.sha256(stdout).hexdigest()}
tmp = job / "done.supervisor.tmp"
with tmp.open("w") as stream:
    json.dump(record, stream)
    stream.flush()
    os.fsync(stream.fileno())
try:
    os.link(tmp, job / "done.json")
except FileExistsError:
    pass
finally:
    tmp.unlink(missing_ok=True)
STOPPY
STOP
        chmod 0700 "$JOB_DIR/stop.sh"
    }

    # Linger before the first job on this connection. Retries that only observe
    # (done / active / lost) must not fail if linger is later disabled.
    # Take the per-job lock before the new-versus-existing decision so two
    # concurrent first submits cannot both claim (CON-11).
    mkdir -p "$ROOT"
    # Global native order: publication -> job -> admission. Queue owns its
    # separate singleton lock first and takes publication/job nonblocking.
    # Publish the entire queued payload under one fence, including retries.
    # FD 7 is closed in descendants; publication ends before consumer launch.
    exec 7>>"$ROOT/.queue-publication.lock"
    flock -w 30 7 || { echo "remote_agent: vm_queue_publication_lock_timeout" >&2; exit 75; }
    mkdir -p "$ROOT/.jobs"
    mkdir -p "$JOB_DIR"
    chmod 0700 "$JOB_DIR"
    exec 9>"$JOB_DIR/.lock"
    flock -w 30 9 || { echo "remote_agent: submit_lock_timeout" >&2; exit 75; }

    _state=""
    if [ -f "$JOB_DIR/state.json" ]; then
        _state="$(sed -n 's/.*"state":"\([^"]*\)".*/\1/p' "$JOB_DIR/state.json" | head -n1 || true)"
    fi
    if [ -f "$JOB_DIR/done.json" ]; then
        _submit_print "done"
        exit 0
    fi
    if _submit_unit_active; then
        if [ "$_state" != "submitted" ]; then
            _submit_write_state submitted
        fi
        _submit_print "submitted"
        exit 0
    fi
    if [ -f "$JOB_DIR/started" ]; then
        _submit_print "lost"
        exit 0
    fi

    if [ "$_state" = queued ] && _submit_queue_fresh; then
        _submit_print queued
        exit 0
    fi
    # Routine dispatch has no authority to install/promote shared runtime.
    # Preserve existing queued payloads on failure, including their queue age.
    if ! _submit_queue_fresh; then
        echo "remote_agent: vm_queue_unavailable; fresh installed queue required" >&2
        exit 75
    fi
    _submit_require_linger
    if [ -z "$_state" ]; then
        _submit_write_state claiming
        _submit_crash_if claiming
    fi
    _submit_write_body_and_env
    _submit_write_start
    # The heartbeat may have expired while staging. Never claim publication
    # by falling back to a direct unit start.
    if ! _submit_queue_fresh; then
        echo "remote_agent: vm_queue_unavailable; queue expired during publication" >&2
        exit 75
    fi
    _submit_write_state ready
    _submit_crash_if ready
    _submit_write_state queued
    _submit_print queued
    exit 0
}
# END REMOTE_SUBMIT_JOB

_lane_scope_cgroup_members() {
    local cg="${1:-}" root="${WORKBAY_REMOTE_AGENT_CGROUP_ROOT:-/sys/fs/cgroup}"
    local proc="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}" procs pid comm out=""
    [ -n "$cg" ] || return 0
    procs="$root$cg/cgroup.procs"
    [ -e "$procs" ] || return 0
    [ -f "$procs" ] && [ -r "$procs" ] || return 2
    while IFS= read -r pid || [ -n "$pid" ]; do
        case "$pid" in ''|*[!0-9]*) continue ;; esac
        comm=unknown
        if [ -r "$proc/$pid/comm" ]; then
            comm="$(tr -d '\n' < "$proc/$pid/comm")"
        fi
        out="${out:+$out,}$pid:$comm"
    done < "$procs" || return 2
    printf '%s' "$out"
}

# BEGIN REMOTE_JOB_OPS
# Host-testable VM helpers for status (batched or --list), collect, cancel, and sweep.
# Nested helpers are defined at call time so `declare -f _remote_job_ops` is
# the single payload shipped over one ssh round trip (RES-12).
_remote_job_ops() {
    set -euo pipefail
    : "${AGENT_ROOT:?}"
    case "$AGENT_ROOT" in /*) ROOT="$AGENT_ROOT" ;; *) ROOT="$HOME/$AGENT_ROOT" ;; esac

    _remote_job_observe_unit() {
        _unit="${1:-}"
        _unit_obs=error
        [ -n "$_unit" ] || return 0
        _obs_rc=0
        _obs_out="$(timeout 10 systemctl --user is-active "$_unit" 2>/dev/null)" || _obs_rc=$?
        _obs_out="$(printf '%s' "$_obs_out" | tr -d '\r' | head -n1 || true)"
        case "$_obs_out" in
            active|activating) _unit_obs=active ;;
            inactive|failed) _unit_obs=inactive ;;
            *) _unit_obs=error ;;
        esac
        case "$_obs_rc" in
            124) _unit_obs=error ;;
        esac
    }

    _remote_job_unit_active() {
        _remote_job_observe_unit "${1:-}"
        [ "$_unit_obs" = "active" ]
    }

    _remote_job_json_field() {
        _f="${1:-}"
        _k="${2:-}"
        [ -f "$_f" ] || return 0
        _json_rc=0
        set +e
        WORKBAY_JSON_FILE="$_f" WORKBAY_JSON_KEY="$_k" timeout 10 python3 - <<'PY'
import json
import os
import sys

path = os.environ.get("WORKBAY_JSON_FILE", "")
key = os.environ.get("WORKBAY_JSON_KEY", "")
try:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
except Exception:
    sys.exit(0)
if not isinstance(data, dict) or key not in data:
    sys.exit(0)
value = data[key]
if value is None:
    sys.exit(0)
print(value)
PY
        _json_rc=$?
        set -e
        [ "$_json_rc" -eq 0 ] || true
    }

    _remote_job_file_age_s() {
        _path="${1:-}"
        [ -e "$_path" ] || return 1
        _now="$(date +%s)"
        _mt="$(stat -c %Y "$_path" 2>/dev/null || true)"
        case "$_mt" in
            ''|*[!0-9]*) return 1 ;;
        esac
        echo $((_now - _mt))
    }

    _remote_job_iso_age_s() {
        _iso="${1:-}"
        [ -n "$_iso" ] || return 1
        _epoch="$(date -u -d "$_iso" +%s 2>/dev/null || true)"
        case "$_epoch" in
            ''|*[!0-9]*) return 1 ;;
        esac
        _now="$(date +%s)"
        echo $((_now - _epoch))
    }

    _remote_queue_stale() {
        _qs="$ROOT/.queue-state.json"
        if [ ! -f "$_qs" ]; then
            echo 1
            return 0
        fi
        _age="$(_remote_job_file_age_s "$_qs" || echo 99999)"
        if [ "$_age" -gt 120 ]; then
            echo 1
        else
            echo 0
        fi
    }

    _remote_job_compute_state() {
        JOB_DIR="$1"
        _out_state="unknown"
        _out_unit=""
        _out_unit_active=0
        _out_unit_obs=""
        _out_rc=""
        _out_hb=""
        if [ ! -d "$JOB_DIR" ]; then
            return 0
        fi
        _state=""
        _unit=""
        if [ -f "$JOB_DIR/state.json" ]; then
            _state="$(_remote_job_json_field "$JOB_DIR/state.json" state)"
            _unit="$(_remote_job_json_field "$JOB_DIR/state.json" unit)"
        fi
        _out_unit="$_unit"
        _out_unit_obs=""
        if [ -n "$_unit" ]; then
            _remote_job_observe_unit "$_unit"
            _out_unit_obs="$_unit_obs"
        else
            _out_unit_obs="inactive"
        fi
        if [ "$_out_unit_obs" = "active" ]; then
            _out_unit_active=1
        fi
        if [ -f "$JOB_DIR/heartbeat" ]; then
            _out_hb="$(_remote_job_file_age_s "$JOB_DIR/heartbeat" || true)"
        fi
        if [ -f "$JOB_DIR/done.json" ]; then
            _out_state="done"
            _out_rc="$(_remote_job_json_field "$JOB_DIR/done.json" rc)"
            return 0
        fi
        if [ "$_state" = "queued" ]; then
            _out_state="queued"
            return 0
        fi
        _started=0
        [ -f "$JOB_DIR/started" ] && _started=1
        if [ "$_out_unit_obs" = "error" ]; then
            case "$_state" in
                claiming|ready) _out_state="claiming" ;;
                submitted|running) _out_state="$_state" ;;
                *) _out_state="${_state:-unknown}" ;;
            esac
            [ -n "$_out_state" ] || _out_state="unknown"
            return 0
        fi
        if [ "$_out_unit_obs" = "inactive" ]; then
            if [ "$_state" = "submitted" ] || [ "$_state" = "running" ] || [ "$_started" -eq 1 ]; then
                _out_state="lost"
                return 0
            fi
        fi
        if [ "$_state" = "claiming" ] || [ "$_state" = "ready" ]; then
            _out_state="claiming"
            return 0
        fi
        if [ "$_out_unit_active" -eq 1 ]; then
            if [ "$_started" -eq 1 ] || [ "$_state" = "running" ]; then
                _out_state="running"
            else
                _out_state="submitted"
            fi
            return 0
        fi
        _out_state="unknown"
    }

    _remote_job_emit_status() {
        _jid="$1"
        JOB_DIR="$ROOT/.jobs/${_jid}"
        _remote_job_compute_state "$JOB_DIR"
        _qs="${QUEUE_STALE:-0}"
        _ua=false
        [ "${_out_unit_active:-0}" -eq 1 ] && _ua=true
        _qjson=false
        [ "$_qs" -eq 1 ] && _qjson=true
        _hb_json=null
        case "${_out_hb:-}" in
            ''|*[!0-9]*) ;;
            *) _hb_json=$_out_hb ;;
        esac
        _rc_json=null
        case "${_out_rc:-}" in
            ''|*[!0-9-]*) ;;
            *) _rc_json=$_out_rc ;;
        esac
        _obs_err_json=null
        if [ "${_out_unit_obs:-}" = "error" ]; then
            _obs_err_json='"systemctl_is_active_failed"'
            echo "remote_agent: observer_failed job_id=${_jid}" >&2
        fi
        _obs="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '{"job_id":"%s","state":"%s","heartbeat_age_s":%s,"unit_active":%s,"rc":%s,"queue_stale":%s,"observed_at":"%s","schema_version":1,"observer_error":%s}\n' \
            "$_jid" "$_out_state" "$_hb_json" "$_ua" "$_rc_json" "$_qjson" "$_obs" "$_obs_err_json"
    }

    # Sweep is a destructive accessor: publication -> job, like native
    # publishers. The subshell releases both descriptors on every outcome.
    # Nonblocking acquisition defers busy identities without waiting on a
    # completion writer or unlinked lock inode. No eligibility survives from
    # outside this lifetime; all observations and deletion occur inside it.
    _remote_job_sweep_one() (
        _jd="$1"
        _jid="${_jd##*/}"
        _remote_job_sweep_defer() {
            echo "remote_agent: sweep_deferred job_id=${_jid} reason=$1" >&2
        }
        if ! exec 7>>"$ROOT/.queue-publication.lock"; then
            _remote_job_sweep_defer publication_lock_unavailable
            return 0
        fi
        if ! flock -n 7; then
            _remote_job_sweep_defer publication_busy
            return 0
        fi
        [ -d "$_jd" ] && [ ! -L "$_jd" ] || return 0
        # Older jobs may predate .lock. Preserve their directory retention
        # clock when creating that inode; publication owns this creation.
        _dir_mtime="$(stat -c %Y "$_jd" 2>/dev/null || true)"
        if [ -L "$_jd/.lock" ] || { [ -e "$_jd/.lock" ] && [ ! -f "$_jd/.lock" ]; }; then
            _remote_job_sweep_defer job_lock_unavailable
            return 0
        fi
        if ! exec 9>>"$_jd/.lock"; then
            _remote_job_sweep_defer job_lock_unavailable
            return 0
        fi
        if ! flock -n 9; then
            _remote_job_sweep_defer job_busy
            return 0
        fi
        # One strict, fresh snapshot. Unlike read-only status, sweep must
        # distinguish an unreadable/invalid record from an absent field.
        if ! _snapshot="$(WORKBAY_SWEEP_DIR="$_jd" timeout 10 python3 - <<'PY_SWEEP'
import json
import os
from pathlib import Path

job = Path(os.environ["WORKBAY_SWEEP_DIR"])
def record(name):
    path = job / name
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file():
        raise ValueError("invalid sweep record")
    with path.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("invalid sweep record")
    return data
state = record("state.json") or {}
done = record("done.json")
values = [state.get("state", ""), state.get("unit", ""), state.get("collected_at", "")]
if any(not isinstance(v, str) or len(v) > 256 or any(c in v for c in "|\r\n") for v in values):
    raise ValueError("invalid sweep field")
if done is not None and (not isinstance(done.get("rc"), int) or isinstance(done["rc"], bool)):
    raise ValueError("invalid completion record")
print("|".join(values + [str(int(done is not None))]))
PY_SWEEP
        )"; then
            _remote_job_sweep_defer record_unavailable
            return 0
        fi
        IFS='|' read -r _state _unit _collected _done <<< "$_snapshot"
        _age=""
        if [ -n "$_collected" ]; then
            _age="$(_remote_job_iso_age_s "$_collected" || true)"
            if [ -z "$_age" ]; then
                _remote_job_sweep_defer collection_time_unavailable
                return 0
            fi
        fi
        _unit_obs=inactive
        [ -z "$_unit" ] || _remote_job_observe_unit "$_unit"
        if [ "$_unit_obs" != inactive ]; then
            _remote_job_sweep_defer "unit_${_unit_obs}"
            return 0
        fi
        # Native expiry/cancel and launch reconciliation can finish a job
        # without replacing its queued/launching state. The locked, validated
        # completion authorizes terminal retention; without it these known
        # pending identities remain in flight.
        case "$_state" in
            queued|claiming|ready) [ "$_done" = 1 ] || return 0 ;;
            launching) [ "$_done" = 1 ] || return 0 ;;
            done|submitted|running) ;;
            *) _remote_job_sweep_defer identity_unknown; return 0 ;;
        esac
        if [ -n "$_collected" ] && [ "$_age" -gt 86400 ]; then
            : # Collected terminal/lost custody expires after 24 hours.
        elif [ "$_done" = 1 ]; then
            if [ -z "$_collected" ]; then
                _age="$(_remote_job_file_age_s "$_jd/done.json" || echo 0)"
                if [ "$_age" -gt 604800 ]; then
                    printf 'uncollected_stale %s age_s=%s\n' "$_jid" "$_age"
                fi
            fi
            return 0
        elif [ "$_state" = submitted ] || [ "$_state" = running ]; then
            # An inactive submitted/running unit is lost, as in status.
            case "$_dir_mtime" in
                ''|*[!0-9]*) _remote_job_sweep_defer retention_time_unavailable; return 0 ;;
            esac
            _age="$(( $(date +%s) - _dir_mtime ))"
            [ "$_age" -gt 604800 ] || return 0
        else
            _remote_job_sweep_defer identity_unknown
            return 0
        fi
        if ! rm -rf -- "$_jd"; then
            echo "remote_agent: sweep_delete_failed job_id=${_jid}" >&2
            return 1
        fi
        printf 'removed %s age_s=%s\n' "$_jid" "$_age"
    )

    _remote_job_sweep() {
        [ -d "$ROOT/.jobs" ] || return 0
        for _jd in "$ROOT/.jobs"/*; do
            [ -d "$_jd" ] || continue
            _remote_job_sweep_one "$_jd"
        done
    }

    # `status --list`: read-only enumeration of $ROOT/.jobs (never the sweep).
    # One row per valid job dir, printed by _remote_job_emit_status (the same
    # row `--job-id` prints; never a second serializer), plus one typed error
    # row per entry that cannot be reported as a job:
    #   {"error":CODE,"job_id":ID|null,"job_dir":NAME,"file":F|null,
    #    "detail":D|null,"schema_version":1,"observed_at":TS}
    # Error rows carry no "state" key, so they cannot be read as a job state,
    # and each is echoed on stderr as `job_list_error` [OBS-08]. CODE is one of
    # invalid_job_name, job_not_directory, job_dir_unreadable,
    # status_file_missing, status_file_unreadable, status_file_invalid,
    # probe_unavailable, list_truncated. Row-level errors exit 0 (the rows are
    # the report). A listing that could not look at all (jobs_dir_unreadable,
    # jobs_dir_not_directory: rc 1), whose status probe cannot run on this VM
    # (probe_unavailable: python3 missing, timed out or not runnable; one row
    # naming where it stopped, rc 1, never N per-job errors) or is partial
    # (list_truncated: rc 3) exits non-zero so it never reads as "no jobs".
    # Bounded: at most STATUS_LIST_MAX rows (default 1000, ceiling 10000) and
    # STATUS_LIST_DEADLINE_S seconds (default 90, ceiling 600), both forwarded by
    # the CLI `status --list` from the operator's environment; a value the VM
    # changes (junk, zero, over the ceiling) is reported on stderr as
    # list_bound_adjusted with the bound in force. No network; every probe has
    # its own timeout and non-regular status files are never opened
    # [API-01][RES-13].
    # Resume: capture stderr `list_continuation next_cursor=TOKEN
    # next_action=resume|retry`, then run STATUS_LIST_AFTER=TOKEN with the same
    # status --list command/bounds/root. Omit AFTER (or use start) for a new
    # census. Exit 0 means this traversal completed; 3 is still partial, never
    # empty. A retry cursor precedes the unavailable probe's unreported entry.
    # Order is unfinished then completion history, normal/dot/double-dot groups,
    # C-byte basename order within each group. Tokens also cover corrupt names.
    # Unchanged inventories enumerate each entry once. This is a live traversal,
    # not a snapshot: additions/renames or done.json transitions across the
    # cursor can require a fresh census and may repeat entries. Do not combine
    # pages into an atomic census or infer absence/death from a partial page.
    # A cursor is a position, not a path or deletion/retention authorization.
    _remote_job_id_valid() {
        _jv_re='^j[0123456789abcdef]{20}$'
        [[ "${1-}" =~ $_jv_re ]]
    }

    _remote_job_json_str() {
        _js_out="$(WORKBAY_JSON_VALUE="${1-}" timeout 10 python3 -c 'import json, os, sys
sys.stdout.write(json.dumps(os.environ.get("WORKBAY_JSON_VALUE", "")))' 2>/dev/null)" || _js_out=""
        if [ -z "$_js_out" ]; then
            # Python only formats names; if it is unavailable, preserve identity
            # with a bytewise hex discriminator instead of one shared placeholder.
            _js_hex="$(printf '%s' "${1-}" | LC_ALL=C od -An -v -tx1 2>/dev/null | tr -d '[:space:]' 2>/dev/null)" || _js_hex=""
            if [ -n "$_js_hex" ]; then
                _js_out="\"unrenderable_hex_${_js_hex}\""
            else
                _js_out="\"unrenderable_entry_${_lst_rows:-0}\""
            fi
        fi
        printf '%s' "$_js_out"
    }

    _remote_job_list_error() {
        _le_code="$1"
        _le_name="${2-}"
        _le_file="${3-}"
        _le_detail="${4-}"
        _le_name_json=null
        _le_id_json=null
        if [ -n "$_le_name" ]; then
            if _remote_job_id_valid "$_le_name"; then
                # A valid id is plain hex: no escaping, so no python to fail.
                _le_name_json="\"${_le_name}\""
                _le_id_json="$_le_name_json"
            else
                _le_name_json="$(_remote_job_json_str "$_le_name")"
            fi
        fi
        _le_file_json=null
        [ -z "$_le_file" ] || _le_file_json="\"${_le_file}\""
        _le_detail_json=null
        [ -z "$_le_detail" ] || _le_detail_json="\"${_le_detail}\""
        _le_obs="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf '{"error":"%s","job_id":%s,"job_dir":%s,"file":%s,"detail":%s,"schema_version":1,"observed_at":"%s"}\n' \
            "$_le_code" "$_le_id_json" "$_le_name_json" "$_le_file_json" "$_le_detail_json" "$_le_obs"
        echo "remote_agent: job_list_error error=${_le_code} job_dir=${_le_name_json}${_le_file:+ file=${_le_file}}${_le_detail:+ detail=${_le_detail}}" >&2
    }

    # Echo VALUE as a base-10 int clamped to [FLOOR, CEILING]. Junk, empty and
    # below-floor fall back to DEFAULT. Leading zeros are dropped, and more than
    # 9 significant digits is over any ceiling here and never reaches arithmetic
    # (overflow). Args: VALUE DEFAULT FLOOR CEILING.
    _remote_job_clamp_int() {
        _ci_v="${1-}"
        case "$_ci_v" in
            ''|*[!0-9]*)
                echo "$2"
                return 0
                ;;
        esac
        _ci_v="${_ci_v#"${_ci_v%%[!0]*}"}"
        [ -n "$_ci_v" ] || _ci_v=0
        if [ "${#_ci_v}" -gt 9 ]; then
            echo "$4"
            return 0
        fi
        if [ "$_ci_v" -lt "$3" ]; then
            echo "$2"
            return 0
        fi
        [ "$_ci_v" -le "$4" ] || _ci_v="$4"
        echo "$_ci_v"
    }

    # stderr note when the bound in force differs from what was asked for, so a
    # clamped or defaulted limit is never silent. Args: NAME REQUESTED EFFECTIVE.
    _remote_job_bound_note() {
        [ -n "${2-}" ] && [ "$2" != "$3" ] || return 0
        printf 'remote_agent: list_bound_adjusted var=%s requested=%q effective=%s\n' "$1" "$2" "$3" >&2
    }

    # Echo ok|missing|unreadable|invalid|unavailable:RC for one status input
    # file. Only a regular, non-symlink, readable file is ever opened (a FIFO
    # would block). The probe maps every per-file failure to exit 1, so any other
    # python exit (127 missing, 124 timeout, 126, a crash) means the probe could
    # not run on this VM at all: unavailable, never a verdict on this file.
    # $2 = key that must be a short safe string; $3 = key that must be an int.
    _remote_job_file_probe() {
        _pf="${1-}"
        if [ ! -e "$_pf" ] && [ ! -L "$_pf" ]; then
            echo missing
            return 0
        fi
        if [ -L "$_pf" ] || [ ! -f "$_pf" ] || [ ! -r "$_pf" ]; then
            echo unreadable
            return 0
        fi
        _pf_rc=0
        WORKBAY_JSON_FILE="$_pf" WORKBAY_JSON_SAFE_KEY="${2-}" WORKBAY_JSON_INT_KEY="${3-}" timeout 10 python3 -c 'import json, os, re, sys
try:
    with open(os.environ["WORKBAY_JSON_FILE"], encoding="utf-8") as fh:
        data = json.load(fh)
except Exception:
    sys.exit(1)
if not isinstance(data, dict):
    sys.exit(1)
safe = os.environ.get("WORKBAY_JSON_SAFE_KEY", "")
if safe and safe in data:
    value = data[safe]
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{0,64}", value):
        sys.exit(1)
integer = os.environ.get("WORKBAY_JSON_INT_KEY", "")
if integer and data.get(integer) is not None:
    value = data[integer]
    if not isinstance(value, int) or isinstance(value, bool):
        sys.exit(1)
sys.exit(0)' >/dev/null 2>&1 || _pf_rc=$?
        case "$_pf_rc" in
            0) echo ok ;;
            1) echo invalid ;;
            *) echo "unavailable:${_pf_rc}" ;;
        esac
    }

    # Act on one probe verdict (args: JOB_DIR FILE VERDICT). Returns 0 when the
    # file is usable (ok|missing) and 1 after printing its per-job error row. An
    # unavailable probe is a property of the VM, so it ends the whole listing.
    _remote_job_list_verdict() {
        case "$3" in
            ok|missing) return 0 ;;
            unavailable:*)
                _remote_job_list_continuation retry
                _remote_job_list_error probe_unavailable "$1" "$2" "probe_rc=${3#unavailable:}"
                exit 1
                ;;
        esac
        _remote_job_list_error "status_file_${3}" "$1" "$2"
        return 1
    }

    _remote_job_list_one() {
        _lo_name="$1"
        _lo_path="$2"
        if ! _remote_job_id_valid "$_lo_name"; then
            _remote_job_list_error invalid_job_name "$_lo_name"
            return 0
        fi
        if [ -L "$_lo_path" ] || [ ! -d "$_lo_path" ]; then
            _remote_job_list_error job_not_directory "$_lo_name"
            return 0
        fi
        if [ ! -r "$_lo_path" ] || [ ! -x "$_lo_path" ]; then
            _remote_job_list_error job_dir_unreadable "$_lo_name"
            return 0
        fi
        _lo_state="$(_remote_job_file_probe "$_lo_path/state.json" state)"
        _remote_job_list_verdict "$_lo_name" state.json "$_lo_state" || return 0
        _lo_done="$(_remote_job_file_probe "$_lo_path/done.json" "" rc)"
        _remote_job_list_verdict "$_lo_name" done.json "$_lo_done" || return 0
        if [ "$_lo_state" = missing ] && [ "$_lo_done" = missing ]; then
            _remote_job_list_error status_file_missing "$_lo_name" state.json
            return 0
        fi
        _remote_job_emit_status "$_lo_name"
    }

    # Cursor v1: PHASE+GLOB-GROUP '-' hex(basename). The phase is 0 for
    # entries without done.json and 1 for completion history. Groups are normal,
    # dot, double-dot names; each is C-byte ordered by Bash pathname expansion.
    # No paths, NUL bytes, noncanonical hex, or ambiguous groups are accepted.
    _remote_job_list_cursor() {
        _lc_token="${STATUS_LIST_AFTER-start}"
        _lc_phase=0
        _lc_group=0
        _lc_name=""
        [ "$_lc_token" != start ] || return 0
        _lc_re='^[01][012]-([0-9a-f]{2}){1,255}$'
        [[ "$_lc_token" =~ $_lc_re ]] || return 1
        _lc_phase="${_lc_token:0:1}"
        _lc_group="${_lc_token:1:1}"
        _lc_hex="${_lc_token#*-}"
        while [ -n "$_lc_hex" ]; do
            _lc_byte="${_lc_hex:0:2}"
            case "$_lc_byte" in 00|2f) return 1 ;; esac
            printf -v _lc_char '%b' "\\x${_lc_byte}"
            _lc_name="${_lc_name}${_lc_char}"
            _lc_hex="${_lc_hex:2}"
        done
        case "$_lc_name" in ''|.|..) return 1 ;; esac
        _remote_job_list_group "$_lc_name"
        [ "$_lc_group" = "$_lg_group" ]
    }

    _remote_job_list_group() {
        case "$1" in
            ..*) _lg_group=2 ;;
            .*) _lg_group=1 ;;
            *) _lg_group=0 ;;
        esac
    }

    # stderr metadata preserves the established stdout job/error row schema.
    # Advance only after a row is emitted, including a typed per-entry error.
    # A VM probe failure retries its entry rather than skipping it on resume.
    _remote_job_list_continuation() {
        printf 'remote_agent: list_continuation next_cursor=%s next_action=%s\n' \
            "${_lst_cursor:-start}" "${1:-resume}" >&2
    }

    _remote_job_list() {
        if [ "${STATUS_SWEEP:-0}" = 1 ]; then
            echo "remote_agent: list_sweep_refused: --list is read-only; run status --sweep separately" >&2
            exit 2
        fi
        if [ -n "${STATUS_JOB_IDS:-}" ]; then
            echo "remote_agent: list_job_id_refused: --list enumerates every job; do not pass --job-id" >&2
            exit 2
        fi
        # Locale is function-local: both pathname order and cursor comparison
        # must use bytes, independent of the caller's locale.
        local LC_ALL=C
        export LC_ALL
        if ! _remote_job_list_cursor; then
            echo "remote_agent: list_cursor_invalid: STATUS_LIST_AFTER must be start or a canonical basename cursor" >&2
            exit 2
        fi
        _lst_cursor="${STATUS_LIST_AFTER-start}"
        _lst_dir="$ROOT/.jobs"
        if [ ! -e "$_lst_dir" ] && [ ! -L "$_lst_dir" ]; then
            return 0
        fi
        if [ -L "$_lst_dir" ] || [ ! -d "$_lst_dir" ]; then
            echo "remote_agent: jobs_dir_not_directory path=${_lst_dir}" >&2
            exit 1
        fi
        if [ ! -r "$_lst_dir" ] || [ ! -x "$_lst_dir" ]; then
            echo "remote_agent: jobs_dir_unreadable path=${_lst_dir}" >&2
            exit 1
        fi
        # Junk or zero falls back to the default; huge values clamp to the
        # ceiling; either is reported on stderr, never silent.
        _lst_max="$(_remote_job_clamp_int "${STATUS_LIST_MAX-}" 1000 1 10000)"
        _remote_job_bound_note STATUS_LIST_MAX "${STATUS_LIST_MAX-}" "$_lst_max"
        _lst_deadline="$(_remote_job_clamp_int "${STATUS_LIST_DEADLINE_S-}" 90 1 600)"
        _remote_job_bound_note STATUS_LIST_DEADLINE_S "${STATUS_LIST_DEADLINE_S-}" "$_lst_deadline"
        _lst_t0=$SECONDS
        _lst_rows=0
        # Cheap existence tests put unfinished/ambiguous entries ahead of
        # history without parsing/probing every historical record first.
        for _lst_phase in 0 1; do
            for _lst_entry in "$_lst_dir"/* "$_lst_dir"/.[!.]* "$_lst_dir"/..?*; do
                [ -e "$_lst_entry" ] || [ -L "$_lst_entry" ] || continue
                _lst_done=0
                if [ -e "$_lst_entry/done.json" ] || [ -L "$_lst_entry/done.json" ]; then
                    _lst_done=1
                fi
                [ "$_lst_done" = "$_lst_phase" ] || continue
                _lst_name="${_lst_entry##*/}"
                _remote_job_list_group "$_lst_name"
                _lst_group="$_lg_group"
                if [ "$_lst_phase" -lt "$_lc_phase" ]; then continue; fi
                if [ "$_lst_phase" = "$_lc_phase" ]; then
                    if [ "$_lst_group" -lt "$_lc_group" ]; then continue; fi
                    if [ "$_lst_group" = "$_lc_group" ] && \
                        { [ "$_lst_name" = "$_lc_name" ] || [[ "$_lst_name" < "$_lc_name" ]]; }; then
                        continue
                    fi
                fi
                if [ "$_lst_rows" -ge "$_lst_max" ]; then
                    _remote_job_list_continuation
                    _remote_job_list_error list_truncated "" "" "max_rows=${_lst_max}"
                    exit 3
                fi
                if [ $((SECONDS - _lst_t0)) -ge "$_lst_deadline" ]; then
                    _remote_job_list_continuation
                    _remote_job_list_error list_truncated "" "" "deadline_s=${_lst_deadline}"
                    exit 3
                fi
                _remote_job_list_one "$_lst_name" "$_lst_entry"
                _lst_rows=$((_lst_rows + 1))
                _lst_hex="$(printf '%s' "$_lst_name" | LC_ALL=C od -An -v -tx1 | tr -d '[:space:]')"
                _lst_cursor="${_lst_phase}${_lst_group}-${_lst_hex}"
            done
        done
    }

    _remote_job_status() {
        QUEUE_STALE="$(_remote_queue_stale)"
        if [ "${STATUS_LIST:-0}" = 1 ]; then
            _remote_job_list
            return 0
        fi
        if [ "${STATUS_SWEEP:-0}" = 1 ]; then
            _remote_job_sweep
        fi
        for _jid in ${STATUS_JOB_IDS:-}; do
            _remote_job_emit_status "$_jid"
        done
    }

    _remote_job_collect() {
        : "${JOB_ID:?}"
        JOB_DIR="$ROOT/.jobs/${JOB_ID}"
        if [ "${COLLECT_MODE:-stream}" = "stamp" ]; then
            # Stamp changes queued directory cost and shares its publication
            # fence. Keep the existing stamp schema and empty output protocol.
            mkdir -p "$ROOT"
            exec 7>>"$ROOT/.queue-publication.lock"
            flock -w 30 7 || { echo "remote_agent: vm_queue_publication_lock_timeout" >&2; exit 75; }
            mkdir -p "$JOB_DIR"
            exec 9>"$JOB_DIR/.lock"
            flock -w 30 9 || { echo "remote_agent: collect_lock_timeout" >&2; exit 75; }
            _now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            _sf="$JOB_DIR/state.json"
            WORKBAY_JSON_FILE="$_sf" WORKBAY_COLLECTED_AT="$_now" timeout 10 python3 - <<'PY'
import json
import os
import sys

path = os.environ["WORKBAY_JSON_FILE"]
ts = os.environ["WORKBAY_COLLECTED_AT"]
data = {}
if os.path.isfile(path):
    try:
        with open(path, encoding="utf-8") as fh:
            loaded = json.load(fh)
        if isinstance(loaded, dict):
            data = loaded
    except Exception:
        data = {}
data["collected_at"] = ts
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(data, fh, separators=(",", ":"))
    fh.write("\n")
os.replace(tmp, path)
PY
            return 0
        fi
        if [ ! -f "$JOB_DIR/done.json" ]; then
            echo "remote_agent: job_not_done" >&2
            exit 75
        fi
        cd "$JOB_DIR"
        _list="done.json"
        [ -f stdout ] && _list="$_list stdout"
        [ -f stderr ] && _list="$_list stderr"
        [ -f artifacts.json ] && _list="$_list artifacts.json artifacts"
        tar -cf - $_list
    }

    _remote_job_drain_probe() {
        local unit="$1" props rc=0 load active cg members
        _drain_observation=""
        _drain_invocation_id=""
        _drain_load=""
        props="$(timeout 5 systemctl --user show -p LoadState -p ActiveState -p ControlGroup -p InvocationID "$unit" 2>/dev/null)" || rc=$?
        if [ "$rc" -ne 0 ]; then
            _drain_observation=probe_failed
            [ "$rc" -ne 124 ] || _drain_observation=probe_timeout
            return 0
        fi
        load="$(printf '%s\n' "$props" | sed -n 's/^LoadState=//p')"
        _drain_load="$load"
        _drain_invocation_id="$(printf '%s\n' "$props" | sed -n 's/^InvocationID=//p')"
        case "$_drain_invocation_id" in
            *[!a-zA-Z0-9_.@:-]*) _drain_observation=probe_failed; _drain_invocation_id=""; _drain_load=""; return 0 ;;
        esac
        [ "$load" != not-found ] || return 0
        active="$(printf '%s\n' "$props" | sed -n 's/^ActiveState=//p')"
        cg="$(printf '%s\n' "$props" | sed -n 's/^ControlGroup=//p')"
        rc=0
        members="$(timeout 5 bash -c "$(declare -f _lane_scope_cgroup_members)"'; _lane_scope_cgroup_members "$1"' bash "$cg")" || rc=$?
        if [ "$rc" -ne 0 ]; then
            _drain_observation=members_unreadable
        elif [ -n "$members" ]; then
            _drain_observation=members_present
        elif [ "$load" = not-found ]; then
            return 0
        elif [ -n "$load" ] && { [ "$active" = inactive ] || [ "$active" = failed ]; }; then
            return 0
        elif [ -z "$load" ] || [ -z "$active" ]; then
            _drain_observation=probe_failed
        else
            _drain_observation=active
        fi
    }

    _remote_job_drain_owned() {
        local nonce unit recorded extra units pending="" sep="" drained="" dsep=""
        local previous="" prior_unit prior_id prior_obs found invocation observation gone
        local scopes="" ssep=""
        nonce="$(timeout 5 sed -n 's/^WORKBAY_DISPATCH_NONCE=//p' "$JOB_DIR/env")" || nonce=""
        units="$(timeout 5 cat "$JOB_DIR/owned-scopes")" || {
            echo "remote_agent: cancel_drain_pending" >&2
            exit 75
        }
        units="$(printf '%s\n' "$units" | LC_ALL=C sort -u)"
        if [ -f "$JOB_DIR/cancel-drain.json" ]; then
            previous="$(WORKBAY_DRAIN_FILE="$JOB_DIR/cancel-drain.json" timeout 5 python3 - <<'DRAINPY'
import json
import os

with open(os.environ["WORKBAY_DRAIN_FILE"], encoding="utf-8") as stream:
    receipt = json.load(stream)
for scope in receipt.get("scopes", receipt.get("units", [])):
    print("|".join((scope["unit"], scope.get("invocation_id", ""), scope["observation"])))
DRAINPY
)" || {
                echo "remote_agent: cancel_drain_pending" >&2
                exit 75
            }
        fi
        while read -r unit recorded extra; do
            [ -n "$nonce" ] && [ "$recorded" = "$nonce" ] || continue
            # Only literal systemd unit names are accepted from the ownership journal.
            case "$unit" in ''|*[!a-zA-Z0-9_.@:-]*) continue ;; esac
            found=0
            prior_id=""
            prior_obs=""
            while IFS='|' read -r prior_unit prior_id prior_obs; do
                if [ "$prior_unit" = "$unit" ]; then found=1; break; fi
            done <<< "$previous"
            invocation=""
            observation=""
            if [ "$found" = 1 ] && [ "$prior_obs" = drained ]; then
                # Released ownership never authorizes even a probe of a reused name.
                invocation="$prior_id"
            else
                _remote_job_drain_probe "$unit"
                invocation="$_drain_invocation_id"
                gone=0
                if [ "$_drain_load" = not-found ]; then
                    gone=1
                elif [ "$found" = 1 ]; then
                    # A retry signals only an instance whose id it proved; an
                    # unknown id proves neither ownership nor replacement.
                    case "$_drain_observation" in
                        probe_failed|probe_timeout) ;;
                        "") gone=1 ;;
                        *)
                            if [ -z "$prior_id" ] || [ -z "$invocation" ]; then
                                _drain_observation=ownership_unproven
                            elif [ "$invocation" != "$prior_id" ]; then
                                gone=1
                            fi
                            ;;
                    esac
                fi
                if [ "$gone" = 1 ]; then
                    # The journal owns the old instance, never its replacement.
                    invocation="$prior_id"
                    _drain_observation=""
                elif [ "$_drain_observation" != probe_failed ] && [ "$_drain_observation" != probe_timeout ] && [ "$_drain_observation" != ownership_unproven ]; then
                    timeout 10 systemctl --user stop "$unit" >/dev/null 2>&1 || true
                    _remote_job_drain_probe "$unit"
                    if [ "$_drain_load" = not-found ] || { [ -n "$invocation" ] && [ "$_drain_load" != "" ] && [ -n "$_drain_invocation_id" ] && [ "$_drain_invocation_id" != "$invocation" ]; }; then
                        _drain_observation=""
                    fi
                    case "$_drain_observation" in
                        active|members_present)
                            # KILL only the recorded instance: an unknown fresh id may
                            # name a replacement, so it stays pending with the prior id.
                            if [ -n "$invocation" ] && [ "$_drain_invocation_id" = "$invocation" ]; then
                                timeout 10 systemctl --user kill -s KILL "$unit" >/dev/null 2>&1 || true
                                _remote_job_drain_probe "$unit"
                                if [ "$_drain_load" = not-found ] || { [ -n "$invocation" ] && [ "$_drain_load" != "" ] && [ -n "$_drain_invocation_id" ] && [ "$_drain_invocation_id" != "$invocation" ]; }; then
                                    _drain_observation=""
                                fi
                            fi
                            ;;
                    esac
                fi
                observation="$_drain_observation"
                # A failed retry probe must retain the previous invocation authority.
                if [ "$found" = 1 ]; then invocation="$prior_id"; fi
            fi
            if [ -n "$observation" ]; then
                pending="$pending$sep{\"unit\":\"$unit\",\"observation\":\"$observation\"}"
                sep=,
            else
                drained="$drained$dsep\"$unit\""
                dsep=,
            fi
            scopes="$scopes$ssep{\"unit\":\"$unit\",\"invocation_id\":\"$invocation\",\"observation\":\"${observation:-drained}\"}"
            ssep=,
        done <<< "$units"
        if [ -n "$pending" ]; then
            # Keep the legacy pending list; scopes also remembers released instances.
            printf '{"state":"drain_pending","job_id":"%s","units":[%s],"scopes":[%s],"observed_at":"%s"}\n' \
                "$JOB_ID" "$pending" "$scopes" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$JOB_DIR/cancel-drain.json.tmp"
            mv "$JOB_DIR/cancel-drain.json.tmp" "$JOB_DIR/cancel-drain.json"
            echo "remote_agent: cancel_drain_pending" >&2
            exit 75
        fi
        _drained_json=",\"drained_scopes\":[$drained]"
    }

    _remote_job_cancel() {
        : "${JOB_ID:?}"
        JOB_DIR="$ROOT/.jobs/${JOB_ID}"
        if [ ! -d "$JOB_DIR" ]; then
            echo "remote_agent: job_not_found" >&2
            exit 2
        fi
        # Cancel must fence eligibility before observing/stopping the unit.
        # Stopping first could race a queued launch into an unobserved unit.
        exec 7>>"$ROOT/.queue-publication.lock"
        flock -w 30 7 || { echo "remote_agent: vm_queue_publication_lock_timeout" >&2; exit 75; }
        exec 9>"$JOB_DIR/.lock"
        flock -w 30 9 || { echo "remote_agent: cancel_lock_timeout" >&2; exit 75; }
        # Completed ownership is inert, even if a scope name has been reused.
        [ ! -f "$JOB_DIR/done.json" ] || return 0
        if [ -f "$JOB_DIR/owned-scopes" ]; then
            printf '{"job_id":"%s","fenced_at":"%s"}\n' \
                "$JOB_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$JOB_DIR/cancel.fence.tmp"
            mv "$JOB_DIR/cancel.fence.tmp" "$JOB_DIR/cancel.fence"
            [ ! -f "$JOB_DIR/done.json" ] || return 0
        fi
        _unit=""
        if [ -f "$JOB_DIR/state.json" ]; then
            _unit="$(_remote_job_json_field "$JOB_DIR/state.json" unit)"
        fi
        # Publication keeps queued eligibility fenced during stop. Let the
        # existing completion writer take the identity lock while systemctl
        # waits for it; reacquire before the terminal publication below.
        flock -u 9
        exec 9>&-
        _stop_rc=0
        if [ -n "$_unit" ]; then
            timeout 10 systemctl --user stop "$_unit" >/dev/null 2>&1 || _stop_rc=$?
            _remote_job_observe_unit "$_unit"
            if [ "$_unit_obs" != "inactive" ]; then
                if [ "$_stop_rc" -eq 124 ]; then
                    echo "remote_agent: cancel_timeout" >&2
                    exit 124
                fi
                echo "remote_agent: cancel_failed" >&2
                exit 1
            fi
        fi
        exec 9>"$JOB_DIR/.lock"
        flock -w 30 9 || { echo "remote_agent: cancel_lock_timeout" >&2; exit 75; }
        [ ! -f "$JOB_DIR/done.json" ] || return 0
        _drained_json=""
        if [ -f "$JOB_DIR/owned-scopes" ]; then
            _remote_job_drain_owned
        fi
        if [ ! -f "$JOB_DIR/done.json" ]; then
            _now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            _bytes=0
            _sha="missing"
            if [ -f "$JOB_DIR/stdout" ]; then
                _bytes="$(wc -c < "$JOB_DIR/stdout" | tr -d ' ')"
                case "$_bytes" in
                    ''|*[!0-9]*) _bytes=0 ;;
                esac
                _sha="$(sha256sum "$JOB_DIR/stdout" 2>/dev/null | awk '{print $1}')"
                [ -n "$_sha" ] || _sha="missing"
            elif [ ! -e "$JOB_DIR/stdout" ] && [ ! -L "$JOB_DIR/stdout" ]; then
                # Public collect represents absent stdout as an empty stream.
                # Non-regular paths must retain the invalid receipt sentinel.
                _sha="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            fi
            printf '{"finished_at":"%s","job_id":"%s","rc":8,"reason":"cancelled","schema_version":1,"stdout_bytes":%s,"stdout_sha256":"%s"%s}\n' \
                "$_now" "$JOB_ID" "${_bytes:-0}" "$_sha" "$_drained_json" > "$JOB_DIR/done.json.tmp"
            if ln "$JOB_DIR/done.json.tmp" "$JOB_DIR/done.json" 2>/dev/null; then
                rm -f "$JOB_DIR/done.json.tmp"
            else
                rm -f "$JOB_DIR/done.json.tmp"
            fi
        fi
    }

    case "${REMOTE_JOB_OP:?}" in
        status) _remote_job_status ;;
        collect) _remote_job_collect ;;
        cancel) _remote_job_cancel ;;
        *)
            echo "remote_agent: unknown_job_op" >&2
            exit 2
            ;;
    esac
}
# END REMOTE_JOB_OPS

# implementation note S2 — argv placeholder resolver (host-testable; injected into the
# remote body via $(declare -f ...) so there is one source of truth).
# Whole-element substitution only; any '{'/'}' elsewhere → exit 7 [WEB-02].
_agent_spec_resolve_argv() {
    local _i _el _brace_off _el_len _half _ex_start _excerpt _brief_inline
    for _i in "${!AGENT_ARGV[@]}"; do
        _el="${AGENT_ARGV[$_i]}"
        case "$_el" in
            '{brief_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_BRIEF_FILE:?agent-spec brief unset}"
                ;;
            '{brief_inline}')
                # cursor-remote: inline staged brief contents into the positional
                # prompt slot. Known-token arm — braces in operator text never
                # hit the exit-7 scan (WEB-02 whole-element contract preserved).
                # $(cat ...) strips every trailing newline; the sentinel byte
                # keeps the staged brief byte-identical through the expand.
                _brief_inline="$(cat "${AGENT_SPEC_BRIEF_FILE:?agent-spec brief unset}"; printf 'x')"
                AGENT_ARGV[$_i]="${_brief_inline%x}"
                ;;
            '{schema_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_SCHEMA_FILE:?agent-spec schema file unset}"
                ;;
            '{schema_inline}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_SCHEMA_INLINE:?agent-spec schema inline unset}"
                ;;
            '{result_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_RESULT_FILE:?agent-spec result unset}"
                ;;
            '{stream_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_STREAM_FILE:?agent-spec stream unset}"
                ;;
            '{run_log}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_RUN_LOG:?agent-spec run log unset}"
                ;;
            '{debug_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_DEBUG_FILE:?agent-spec debug unset}"
                ;;
            '{out_dir}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_OUT_DIR:?agent-spec out dir unset}"
                ;;
            *'{'*|*'}'*)
                # Bounded diagnostic: never dump a multi-kilobyte argv element.
                # Walk char-by-char for the first brace offset (avoid %% patterns
                # that embed brace characters and confuse ${...} parsing).
                # Operator free text is staged out-of-band and referenced via
                # {brief_inline}; a brace here is a malformed recipe token or a
                # version-skewed transported script, not operator brief text.
                _el_len=${#_el}
                _brace_off=0
                while [ "$_brace_off" -lt "$_el_len" ]; do
                    case "${_el:_brace_off:1}" in
                        '{'|'}') break ;;
                    esac
                    _brace_off=$((_brace_off + 1))
                done
                _half=40
                if [ "$_brace_off" -gt "$_half" ]; then
                    _ex_start=$((_brace_off - _half))
                else
                    _ex_start=0
                fi
                _excerpt="${_el:_ex_start:80}"
                echo "remote_agent: invalid placeholder in argv element: index=${_i} brace_offset=${_brace_off} element_len=${_el_len} excerpt=<${_excerpt}> (transported scripts/remote_agent.sh does not resolve this token; rebase the lane branch onto main before dispatching. Host recipe is not at fault. Operator brief text is staged out-of-band and never enters argv.)" >&2
                exit 7
                ;;
        esac
    done
}

cmd="${1:-}"; [ "$#" -gt 0 ] && shift

case "$cmd" in
writable-roots-probe|workspace-write-probe)
    # These arms execute on the VM, where the sandbox actually exists. They
    # deliberately return one complete typed receipt on every path; an SSH or
    # host-side timeout is UNKNOWN and never a bare boolean/refusal.
    exec </dev/null
    _probe_kind="$cmd"
    _probe_timeout="$PREFLIGHT_PROBE_TIMEOUT_S"
    _probe_model="$PREFLIGHT_PROBE_MODEL"
    _probe_nonce="$PREFLIGHT_PROBE_NONCE"
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --timeout) _probe_timeout="${2:-}"; shift 2 ;;
            *) echo "remote_agent: unknown preflight probe arg: $1" >&2; exit 2 ;;
        esac
    done
    case "$_probe_timeout" in *[!0-9]*|""|0) echo "remote_agent: probe timeout must be positive" >&2; exit 2 ;; esac
    _probe_cap="$PREFLIGHT_PROBE_MAX_LIVE"
    if ! command -v timeout >/dev/null 2>&1; then
        printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":false,"verdict":"unknown","reason":"transport_failure","elapsed_s":0,"live_probe_count":0,"head":"","model":"%s","nonce":"%s","sandbox_flags":"workspace-write"}\n' "$_probe_kind" "$_probe_model" "$_probe_nonce"
        exit 0
    fi
    if ! command -v flock >/dev/null 2>&1; then
        printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":false,"verdict":"unknown","reason":"transport_failure","elapsed_s":0,"live_probe_count":0,"head":"","model":"%s","nonce":"%s","sandbox_flags":"workspace-write"}\n' "$_probe_kind" "$_probe_model" "$_probe_nonce"
        exit 0
    fi
    if [ "$_probe_cap" -le 0 ] 2>/dev/null; then
        printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":false,"verdict":"unknown","reason":"probe_capacity","elapsed_s":0,"live_probe_count":0,"head":"","model":"%s","nonce":"%s","sandbox_flags":"workspace-write"}\n' "$_probe_kind" "$_probe_model" "$_probe_nonce"
        exit 0
    fi
    _probe_started=$SECONDS
    _probe_rc=0
    # The remote arm owns its lifetime. The command-level timeout remains in
    # force even if this SSH caller vanishes immediately after spawn.
    # Script delivery is opaque; stdin carries only the causal grant.
    _run_preflight_transport() {
    local _preflight_driver
    _preflight_driver=$(cat <<'PREFLIGHT_DRIVER_EOF'
import base64
import json
import os
import selectors
import shlex
import signal
import subprocess
import sys
import time

# Bounds assume rate-compatible monotonic clocks, timely scheduling and a
# healthy kernel. A missing terminal reply never proves remote disappearance.
budget = float(sys.argv[2])
deadline = time.monotonic() + budget
host_deadline = os.environ.get("WORKBAY_PREFLIGHT_HOST_DEADLINE")
if host_deadline is not None:
    deadline = min(deadline, float(host_deadline))
budget = min(budget, deadline - time.monotonic())
if budget <= 0.5:
    sys.exit(124)
body = sys.stdin.read()
args = sys.argv[1:11]
ownership = None
if os.environ.get("WORKBAY_PROBE_LAUNCH_PATH"):
    ownership = {key: os.environ["WORKBAY_PROBE_" + key.upper()] for key in
                 ("flight_id", "launch_id", "owner_nonce")}
    ownership["host"] = os.environ.get("WORKBAY_REMOTE_GATE_HOST", "")
remote = r'''
import base64, ctypes, json, os, secrets, selectors, signal, subprocess, sys, time
args = json.loads(base64.b64decode(sys.argv[1]))
body = base64.b64decode(sys.argv[2])
ownership = json.loads(base64.b64decode(sys.argv[3])) if len(sys.argv) > 3 else None
# timeout arms the bootstrap watchdog before this process can announce READY.
# Linux VM subreaper: retain orphan ownership through escalation and reaping.
if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
    sys.exit(1)
r0 = time.monotonic()
challenge = secrets.token_hex(16)
ready = [args[5], challenge]
if ownership is not None:
    from pathlib import Path
    if os.environ.get("WORKBAY_PROBE_OWNER_NONCE") != ownership["owner_nonce"]:
        sys.exit(124)
    stat = Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()
    receipt = dict(host=ownership["host"], pid=os.getpid(), uid=os.getuid(),
                   start_time=stat[19],
                   command=Path("/proc/self/cmdline").read_bytes().rstrip(b"\0").replace(b"\0", b" ").decode(),
                   owner_nonce=ownership["owner_nonce"])
    ready.append(dict(flight_id=ownership["flight_id"], launch_id=ownership["launch_id"], receipt=receipt))
print("WORKBAY_READY " + json.dumps(ready), flush=True)
selector = selectors.DefaultSelector()
selector.register(sys.stdin, selectors.EVENT_READ)
line = b""
while b"\n" not in line:
    bootstrap_remaining = min(5, float(args[1])) - (time.monotonic() - r0)
    if bootstrap_remaining <= 0 or not selector.select(bootstrap_remaining):
        sys.exit(124)
    chunk = os.read(sys.stdin.fileno(), 4097)
    if not chunk:
        sys.exit(124)
    line += chunk
    if len(line) > 4096:
        sys.exit(124)
try:
    tag, data = line.decode().split(" ", 1)
    nonce, response, remaining = json.loads(data)
    if tag != "WORKBAY_GRANT" or nonce != args[5] or response != challenge:
        raise ValueError("invalid grant identity")
    if type(remaining) not in (float, int) or not 0 < remaining <= float(args[1]):
        raise ValueError("invalid grant duration")
except Exception:
    sys.exit(124)
lease = min(float(args[1]), remaining - (time.monotonic() - r0))
if lease <= 1.25:
    sys.exit(124)
# An independent timeout retains escalation even when the SSH transport dies.
cancelled = False
def cancel_remote(*_):
    global cancelled
    cancelled = True

signal.signal(signal.SIGTERM, cancel_remote)
proc = None
try:
    if cancelled:
        sys.exit(124)
    proc = subprocess.Popen(["timeout", "-k", "0.5", str(lease - 0.75),
                             "bash", "-s", "--", *args, str(lease - 0.75)], stdin=subprocess.PIPE,
                            start_new_session=True)
    proc.stdin.write(body)
    proc.stdin.close()
    while os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
        if cancelled or selector.select(0.05):
            # EOF, supervisor cancellation and a replay cannot renew the lease.
            cancelled = True
            break
finally:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if proc is not None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        # WNOWAIT pins the leader identity until the last group signal.
        term_deadline = min(r0 + lease, time.monotonic() + 0.25)
        while os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT | os.WNOHANG) is None:
            if time.monotonic() >= term_deadline:
                break
            time.sleep(0.01)
        # The stable supervisor never execs. Every exit after spawn tears down
        # and awaits its isolated child scope, including failed body delivery.
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=max(0.001, min(0.25, r0 + lease - time.monotonic())))
        reap_deadline = min(r0 + lease, time.monotonic() + 0.2)
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if pid == 0:
                if time.monotonic() >= reap_deadline:
                    sys.exit(124)
                time.sleep(0.01)
sys.exit(124 if cancelled or proc.returncode < 0 else proc.returncode)
'''
encoded_args = base64.b64encode(json.dumps(args).encode()).decode()
encoded_body = base64.b64encode(body.encode()).decode()
encoded_ownership = base64.b64encode(json.dumps(ownership).encode()).decode()
# Opaque command delivery leaves stdin exclusively for the one-shot grant.
command = 'exec timeout -k 0.5 ' + str(budget) + ' python3 -c ' + shlex.quote(remote)
command += ' ' + shlex.quote(encoded_args) + ' ' + shlex.quote(encoded_body) + ' ' + shlex.quote(encoded_ownership)
if ownership is not None:
    command = 'export WORKBAY_PROBE_OWNER_NONCE=' + shlex.quote(ownership["owner_nonce"]) + '; ' + command
ssh_argv = list(sys.argv[11:])
if ownership is not None:
    ssh_argv[1:1] = ['-o', 'StrictHostKeyChecking=yes']
proc = subprocess.Popen([*ssh_argv, 'bash', '-c', shlex.quote(command)],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        start_new_session=True)
selector = selectors.DefaultSelector()
selector.register(proc.stdout, selectors.EVENT_READ)
output = bytearray()
granted = False
def cancel_host_transport(*_):
    raise TimeoutError("host cancelled preflight transport")

signal.signal(signal.SIGTERM, cancel_host_transport)
try:
    while True:
        remaining = deadline - time.monotonic() - 0.5
        if remaining <= 0 or not selector.select(remaining):
            raise TimeoutError
        chunk = os.read(proc.stdout.fileno(), 65536)
        if not chunk:
            break
        output.extend(chunk)
        if not granted and b'\n' in output:
            line, _, tail = output.partition(b'\n')
            if line.startswith(b'WORKBAY_READY '):
                ready = json.loads(line[len(b'WORKBAY_READY '):])
                nonce, challenge = ready[:2]
                if nonce != args[5] or not isinstance(challenge, str) or len(challenge) != 32:
                    raise ValueError('invalid READY')
                if ownership is not None:
                    if len(ready) != 3:
                        raise ValueError('missing ownership receipt')
                    frame = ready[2]
                    if frame["flight_id"] != ownership["flight_id"] or frame["launch_id"] != ownership["launch_id"]:
                        raise ValueError('foreign ownership frame')
                    import runpy
                    from pathlib import Path
                    retain = runpy.run_path(os.environ["WORKBAY_PROBE_OWNERSHIP_HELPER"])["retain_receipt"]
                    retain(Path(os.environ["WORKBAY_PROBE_LAUNCH_PATH"]),
                           flight_id=frame["flight_id"], launch_id=frame["launch_id"], receipt=frame["receipt"])
                    # GRANT acknowledges fsynced retention. No child exists
                    # until this succeeds; a lost grant leaves UNKNOWN work.
                elif len(ready) != 2:
                    raise ValueError('unexpected ownership frame')
                remaining = deadline - time.monotonic() - 0.5
                if remaining <= 0:
                    raise TimeoutError
                proc.stdin.write(('WORKBAY_GRANT ' + json.dumps([nonce, challenge, remaining]) + '\n').encode())
                proc.stdin.flush()
                granted = True
                output = bytearray(tail)
        if len(output) > 65536:
            raise ValueError('oversized receipt')
    rc = proc.wait(timeout=max(0.001, deadline - time.monotonic() - 0.5))
    if rc == 0:
        if not granted:
            raise ValueError("receipt without READY/GRANT")
        sys.stdout.buffer.write(output)
    sys.exit(rc if rc >= 0 else 124)
except (TimeoutError, subprocess.TimeoutExpired):
    sys.exit(124)
except (ValueError, OSError):
    sys.exit(1)
finally:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=0.25)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=0.25)
PREFLIGHT_DRIVER_EOF
)
    python3 -c "$_preflight_driver" "$_probe_kind" "$_probe_timeout" "$_probe_model" "$_probe_cap" "$AGENT_ROOT" "$_probe_nonce" "${WORKBAY_PREFLIGHT_PROBE_EFFORT:-low}" "${WORKBAY_PREFLIGHT_PROBE_BINARY:-.local/bin/codex}" "${WORKBAY_PREFLIGHT_PROBE_PATH:-.local/bin}" "$PREFLIGHT_PROBE_SLOT_WAIT_S" "${SSH[@]}" <<'REMOTE_PREFLIGHT_BODY_EOF'
set -u
kind="${1:-}"
bound="${2:-}"
model="${3:-}"
cap="${4:-2}"
agent_root="${5:-workbay-sandbox}"
nonce="${6:-workbay-probe-${BASHPID:-$$}}"
effort="${7:-low}"
slot_wait="${10:-0}"
body_budget="${11:-}"
export WORKBAY_PROBE_NONCE="$nonce"
started="$(date +%s)"
waited_s=0
wait_budget=0
budget_whole=0
if [[ "$slot_wait" =~ ^[0-9]+$ ]] && [[ "$body_budget" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    # Compare decimal magnitudes before arithmetic: an accepted wait may be
    # larger than a signed shell integer, including after leading zeroes.
    slot_wait_decimal="${slot_wait#"${slot_wait%%[!0]*}"}"
    slot_wait_decimal="${slot_wait_decimal:-0}"
    budget_decimal="${body_budget%%.*}"
    budget_decimal="${budget_decimal#"${budget_decimal%%[!0]*}"}"
    budget_decimal="${budget_decimal:-0}"
    # The absolute deadline must fit too, including the current clock value.
    budget_max=$((9223372036854775807 - started))
    if [[ "${#budget_decimal}" -lt "${#budget_max}" ]] ||
       { [[ "${#budget_decimal}" -eq "${#budget_max}" ]] && [[ "$budget_decimal" < "$budget_max" || "$budget_decimal" = "$budget_max" ]]; }; then
        budget_whole=$((10#$budget_decimal))
        budget_half=$((budget_whole / 2))
        wait_budget="$budget_half"
        if [ "${#slot_wait_decimal}" -lt "${#budget_half}" ] ||
           { [ "${#slot_wait_decimal}" -eq "${#budget_half}" ] && [[ "$slot_wait_decimal" < "$budget_half" ]]; }; then
            wait_budget=$((10#$slot_wait_decimal))
        fi
    else
        echo 'remote_agent: probe body budget exceeds the shell integer range; refusing lock waits' >&2
    fi
fi
# Both locks share half the supervisor's effective budget. Setup consumes the
# absolute deadline, which reserves a clock tick and receipt time before timeout.
# A caller without a valid body budget keeps non-blocking acquisition only.
wait_deadline=$((started + budget_whole - 1))
case "$agent_root" in /*) slot_root="$agent_root/.workbay-preflight-probes" ;; *) slot_root="$HOME/$agent_root/.workbay-preflight-probes" ;; esac
mkdir -p -- "$slot_root" 2>/dev/null || true
emit() {
    _ok="$1"; _verdict="$2"; _reason="$3"; _head="${4:-}"; _now="$(date +%s)"
    _elapsed=$((_now - started))
    _waited="${waited_s:-0}"
    if [ "$_waited" -gt "$_elapsed" ]; then _waited="$_elapsed"; fi
    printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":%s,"verdict":"%s","reason":"%s","elapsed_s":%s,"waited_s":%s,"live_probe_count":%s,"head":"%s","model":"%s","nonce":"%s","sandbox_flags":"%s"}\n' \
        "$kind" "$_ok" "$_verdict" "$_reason" "$_elapsed" "$_waited" "${live_count:-0}" "$_head" "$model" "$nonce" "${sandbox_flags:-workspace-write}"
}
_record_probe_wait() {
    local _wait_from="$1" _wait_to="$2" _wait_duration
    _wait_duration=$((_wait_to - _wait_from))
    if [ "$_wait_duration" -gt 0 ]; then waited_s=$((waited_s + _wait_duration)); fi
}
if ! [[ "$cap" =~ ^[0-9]+$ ]] || [ "$cap" -lt 1 ]; then
    live_count=0; emit false unknown probe_capacity; exit 0
fi
# Dynamic redirection closes the descriptor held by the variable; reject any
# non-numeric value before it reaches that redirection, even though the value
# is not re-parsed as shell source.
_close_probe_fd() {
    _close_fd="${1:-}"
    case "$_close_fd" in
        ''|*[!0-9]*) return 0 ;;
        *) exec {_close_fd}>&- 2>/dev/null || true ;;
    esac
}
slot_fd=''; slot_index=''; live_count=0
for _slot in $(seq 0 $((cap - 1))); do
    _slot_path="$slot_root/slot-${_slot}.lock"
    exec {_fd}>"$_slot_path" 2>/dev/null || continue
    if flock -n "$_fd"; then
        slot_fd="$_fd"
        slot_index="$_slot"
        live_count=1
        break
    fi
    _close_probe_fd "$_fd"
done
if [ -z "$slot_fd" ] && [ "$wait_budget" -gt 0 ]; then
    _wait_start_slot=0
    while [ -z "$slot_fd" ]; do
        _wait_now="$(date +%s)"
        _wait_left=$((wait_deadline - _wait_now))
        _wait_allowance=$((wait_budget - waited_s))
        if [ "$_wait_left" -gt "$_wait_allowance" ]; then _wait_left="$_wait_allowance"; fi
        if [ "$_wait_left" -le 0 ]; then break; fi
        _rotation_opened=0
        for ((_wait_offset = 0; _wait_offset < cap; _wait_offset++)); do
            _wait_now="$(date +%s)"
            _wait_left=$((wait_deadline - _wait_now))
            _wait_allowance=$((wait_budget - waited_s))
            if [ "$_wait_left" -gt "$_wait_allowance" ]; then _wait_left="$_wait_allowance"; fi
            if [ "$_wait_left" -le 0 ]; then break; fi
            _wait_slot=$(((_wait_start_slot + _wait_offset) % cap))
            _wait_path="$slot_root/slot-${_wait_slot}.lock"
            exec {_wait_fd}>"$_wait_path" 2>/dev/null || continue
            _rotation_opened=1
            _wait_before="$(date +%s)"
            if [ "$((_wait_before + 1))" -gt "$wait_deadline" ]; then
                _close_probe_fd "$_wait_fd"
                break
            fi
            if flock -w 1 "$_wait_fd"; then
                _wait_after="$(date +%s)"
                _record_probe_wait "$_wait_before" "$_wait_after"
                slot_fd="$_wait_fd"
                slot_index="$_wait_slot"
                live_count=1
                break
            fi
            _wait_after="$(date +%s)"
            _record_probe_wait "$_wait_before" "$_wait_after"
            _close_probe_fd "$_wait_fd"
        done
        if [ -z "$slot_fd" ]; then
            if [ "$_rotation_opened" -eq 0 ]; then break; fi
            _wait_start_slot=$(((_wait_start_slot + 1) % cap))
        fi
    done
fi
if [ -z "$slot_fd" ]; then
    # Another slot may have freed while the last blocking attempt used up the
    # allowance. Exhaustion requires a fresh non-blocking scan of every slot.
    for _slot in $(seq 0 $((cap - 1))); do
        _slot_path="$slot_root/slot-${_slot}.lock"
        exec {_fd}>"$_slot_path" 2>/dev/null || continue
        if flock -n "$_fd"; then
            slot_fd="$_fd"
            slot_index="$_slot"
            live_count=1
            break
        fi
        _close_probe_fd "$_fd"
    done
fi
if [ -z "$slot_fd" ]; then
    live_count="$cap"; emit false unknown probe_capacity; exit 0
fi
for _count_slot in $(seq 0 $((cap - 1))); do
    [ "$_count_slot" = "$slot_index" ] && continue
    _count_path="$slot_root/slot-${_count_slot}.lock"
    exec {_count_fd}>"$_count_path" 2>/dev/null || continue
    if flock -n "$_count_fd"; then
        flock -u "$_count_fd" 2>/dev/null || true
    else
        live_count=$((live_count + 1))
    fi
    _close_probe_fd "$_count_fd"
done
cleanup() {
    # Keep the stable cwd: Codex may retain its project trust entry.
    _close_probe_fd "${cwd_fd:-}"
    # Descendants inherit the lock; do not unlock their shared description.
    _close_probe_fd "$slot_fd"
}
trap cleanup EXIT
# Keep the group leader alive until timeout's reserved KILL escalation. A
# model may exit on TERM while its own children ignore it; returning early
# would cancel timeout's escalation and leave those children behind.
trap 'sleep 2; exit 124' HUP INT TERM
configured_binary="${8:-.local/bin/codex}"
IFS=: read -r -a probe_paths <<< "${9:-.local/bin}"
for ((i=${#probe_paths[@]}-1; i>=0; i--)); do
    entry="${probe_paths[$i]}"
    case "$entry" in /*) ;; *) entry="$HOME/$entry" ;; esac
    export PATH="$entry:$PATH"
done
case "$configured_binary" in
    /*) codex="$configured_binary" ;;
    */*) codex="$HOME/$configured_binary" ;;
    *) codex="$(command -v "$configured_binary" 2>/dev/null || true)" ;;
esac
if [ -z "$codex" ] || [ ! -x "$codex" ]; then
    emit false negative binary_missing
    exit 0
fi
# One cwd for every kind and nonce. Serialize its use, including descendants.
exec {cwd_fd}>"$slot_root/cwd.lock"
if ! flock -n "$cwd_fd"; then
    _wait_now="$(date +%s)"
    _wait_left=$((wait_deadline - _wait_now))
    _wait_allowance=$((wait_budget - waited_s))
    if [ "$_wait_left" -gt "$_wait_allowance" ]; then _wait_left="$_wait_allowance"; fi
    if [ "$_wait_left" -gt 0 ]; then
        _wait_before="$_wait_now"
        if flock -w "$_wait_left" "$cwd_fd"; then
            _wait_after="$(date +%s)"
            _record_probe_wait "$_wait_before" "$_wait_after"
        else
            _wait_after="$(date +%s)"
            _record_probe_wait "$_wait_before" "$_wait_after"
            emit false unknown probe_capacity
            exit 0
        fi
    else
        emit false unknown probe_capacity
        exit 0
    fi
fi
probe_root="$slot_root/cwd"
if [ -L "$probe_root" ] || ! mkdir -p -- "$probe_root"; then
    emit false unknown spawn_error
    exit 0
fi
# Clear previous evidence before either probe (including an interrupted run).
if ! find "$probe_root" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; then
    emit false unknown spawn_error
    exit 0
fi
if ! cd -- "$probe_root"; then
    emit false unknown spawn_error
    exit 0
fi
if [ "$kind" = "writable-roots-probe" ]; then
    if ! git -C "$probe_root" init -b probe >/dev/null 2>&1; then
        emit false unknown executor_failed
        exit 0
    fi
    printf '%s\n' 'workbay writable roots probe' >"$probe_root/probe.txt"
    prompt='Create a commit in the current repository containing probe.txt. Do not modify anything outside this repository.'
    sandbox_flags='workspace-write,writable_roots=.git'
    sandbox_extra='sandbox_workspace_write.writable_roots=[".git"]'
else
    prompt='Create the file .workbay-workspace-write-probe with the contents probe. Do not modify anything outside this directory.'
    sandbox_flags='workspace-write'
    sandbox_extra=''
fi
codex_out="$probe_root/codex.out"
codex_err="$probe_root/codex.err"
if [ -n "$sandbox_extra" ]; then
    if "$codex" exec --ignore-user-config -C . -m "$model" -c "model_reasoning_effort=$effort" -s workspace-write -c "$sandbox_extra" "$prompt" >"$codex_out" 2>"$codex_err" </dev/null; then _rc=0; else _rc=$?; fi
else
    if "$codex" exec --ignore-user-config -C . -m "$model" -c "model_reasoning_effort=$effort" -s workspace-write "$prompt" >"$codex_out" 2>"$codex_err" </dev/null; then _rc=0; else _rc=$?; fi
fi
if [ "$_rc" -eq 124 ] || [ "$_rc" -eq 137 ]; then
    emit false unknown timeout
    exit 0
fi
if [ "$_rc" -ne 0 ]; then
    if grep -Eqi 'permission denied|not permitted|sandbox|writable.roots' "$codex_err" 2>/dev/null; then
        emit false negative sandbox_denied
    else
        emit false unknown executor_failed
    fi
    exit 0
fi
if [ "$kind" = "writable-roots-probe" ]; then
    _head="$(git -C "$probe_root" rev-parse --verify HEAD 2>/dev/null || true)"
    if [ -n "$_head" ]; then emit true positive committed "$_head"; else emit false unknown commit_absent; fi
else
    if [ -f "$probe_root/.workbay-workspace-write-probe" ]; then emit true positive committed; else emit false unknown commit_absent; fi
fi
exit 0
REMOTE_PREFLIGHT_BODY_EOF
    }
    if _probe_output=$(_run_preflight_transport)
    then
        _probe_rc=0
        printf '%s\n' "$_probe_output"
    else
        _probe_rc=$?
    fi
    if [ "$_probe_rc" -ne 0 ]; then
        _probe_reason=transport_failure
        case "$_probe_rc" in 124|137) _probe_reason=timeout ;; esac
        _probe_elapsed=$((SECONDS - _probe_started))
        printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":false,"verdict":"unknown","reason":"%s","cleanup_status":"unknown","retryable":false,"elapsed_s":%s,"live_probe_count":0,"head":"","model":"%s","nonce":"%s","sandbox_flags":"workspace-write"}\n' "$_probe_kind" "$_probe_reason" "$_probe_elapsed" "$_probe_model" "$_probe_nonce"
    fi
    exit 0
    ;;
doctor)
    "${SSH[@]}" 'g="$HOME/.grok/bin/grok"
        [ -x "$g" ] && echo "grok    : $("$g" --version)" || echo "grok    : MISSING (install per runbook)"
        # implementation note: per-backend binary/env paths are AgentSpec data, not doctor
        # hardcodes (vendor-free executor). uv/systemd probes stay host-generic.
        [ -x "$HOME/.local/bin/uv" ] && echo "uv      : $("$HOME/.local/bin/uv" --version)" || echo "uv      : MISSING"
        command -v systemd-run >/dev/null && echo "caps    : systemd-run available" || echo "caps    : systemd-run MISSING (nice/ionice only)"
        root='"$AGENT_ROOT"'; case "$root" in /*) ;; *) root="$HOME/$root" ;; esac
        n=$(ls -1d "$root"/.venv-lane-* 2>/dev/null | wc -l | tr -d " ")
        tot=$(du -csh "$root"/.venv-lane-* 2>/dev/null | tail -1 | cut -f1)
        echo "venvs   : ${n:-0} persisted lane venv(s)${tot:+, ~$tot total} (cap '"$LANE_VENV_CAP"')"'
    # implementation note S5: one `auth` line per AuthPort declared in the orchestrator
    # registry (the same python call scripts/provision_remote_auth.sh makes),
    # not a hardcoded grok line. Presence + kind only — never the value, never
    # the perms of a file that could leak. key_info ports (openrouter) also get a
    # `budget` line (probe reading on the VM) and a `price` line (public list
    # price, curl --max-time 10, "unavailable" on any failure). The rendered
    # script is streamed over ssh stdin under a deny-by-default env so the
    # laptop interpreter tree never sees an operator-exported key.
    # The checkout that holds THIS script (a linked worktree's toplevel, not the
    # git-common-dir root) so the registry matches the script version.
    _doc_root="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || printf '%s' "$repo_root")"
    # Interpreter ladder (S5-M-02): WORKBAY_PYTHON, the checkout's .venv, uv
    # against the in-tree orchestrator project (only when that tree exists —
    # a payload install has no packages/ dir), then a PATH python3 that can
    # import the package. Each rung is tried until one renders; the FIRST
    # stderr line of the last failure is surfaced so a skip is never silent.
    _doc_render='from workbay_orchestrator_mcp.orchestration.backend_registry import render_doctor_auth_script; print(render_doctor_auth_script(), end="")'
    _doc_script=""
    _doc_err=""
    _doc_try() {
        local _out _errf
        _errf=$(mktemp "${TMPDIR:-/tmp}/wb-doctor-py.XXXXXX") || _errf=""
        _out="$(env -i PATH="$PATH" HOME="$HOME" TMPDIR="${TMPDIR:-}" ${UV_CACHE_DIR:+UV_CACHE_DIR="$UV_CACHE_DIR"} \
            "$@" -c "$_doc_render" 2>"${_errf:-/dev/null}")" || _out=""
        if [ -n "$_out" ]; then
            _doc_script="$_out"
        else
            _doc_err="$(head -n 1 "${_errf:-/dev/null}" 2>/dev/null || true)"
            [ -n "$_doc_err" ] || _doc_err="$1: rendered nothing"
        fi
        [ -n "$_errf" ] && rm -f "$_errf"
        [ -n "$_doc_script" ]
    }
    _doc_rungs="none"
    if [ -n "${WORKBAY_PYTHON:-}" ]; then
        _doc_rungs="WORKBAY_PYTHON"
        _doc_try "$WORKBAY_PYTHON" || true
    fi
    if [ -z "$_doc_script" ] && [ -x "$_doc_root/.venv/bin/python" ]; then
        _doc_rungs="$_doc_rungs,.venv"
        _doc_try "$_doc_root/.venv/bin/python" || true
    fi
    if [ -z "$_doc_script" ] && [ -d "$_doc_root/packages/mcp-workbay-orchestrator" ] && command -v uv >/dev/null 2>&1; then
        _doc_rungs="$_doc_rungs,uv"
        _doc_try uv run --frozen --no-sync --project "$_doc_root/packages/mcp-workbay-orchestrator" python || true
    fi
    if [ -z "$_doc_script" ] && command -v python3 >/dev/null 2>&1; then
        _doc_rungs="$_doc_rungs,python3"
        _doc_try python3 || true
    fi
    if [ -n "$_doc_script" ]; then
        printf '%s\n' "$_doc_script" | "${SSH[@]}" 'bash -s'
    else
        echo "auth    : registry unavailable on this host (tried: $_doc_rungs; last error: ${_doc_err:-no interpreter found}); set WORKBAY_PYTHON or install the orchestrator package; per-port auth lines skipped" >&2
    fi
    ;;
reap)
    REAP_IDLE="$SANDBOX_IDLE_SEC"
    REAP_DRY=0
    REAP_ELIGIBLE=""
    REAP_AUTHORIZED_ONLY=0
    REAP_ELIGIBLE_STDIN=0
    REAP_CALLER_BUDGET=""
    REAP_CALLER_BUDGET_GIVEN=0
    _reap_degraded_reason=
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -h|--help)
                cat <<'REAP_USAGE_EOF'
usage: remote_agent.sh reap [--idle-seconds N] [--caller-budget-seconds N] [--dry-run] [--authorized-only] [--eligible-keys-stdin] [--eligible-key K]...
  --idle-seconds N       reclaim only sandboxes idle for more than N seconds
  --caller-budget-seconds N  the caller's wait; the /tmp accounting window ends inside it (absent: default window)
  --dry-run              report what would be reclaimed; delete nothing
  --authorized-only      act only on keys named by --eligible-key or --eligible-keys-stdin
  --eligible-keys-stdin  read authorized lane keys from stdin, one per line (empty input is a valid sweep)
  --eligible-key K       authorize lane key K (repeatable)
REAP_USAGE_EOF
                exit 0
                ;;
            --idle-seconds) REAP_IDLE="${2:-}"; shift 2 ;;
            --caller-budget-seconds) REAP_CALLER_BUDGET="${2:-}"; REAP_CALLER_BUDGET_GIVEN=1; shift 2 ;;
            --dry-run) REAP_DRY=1; shift ;;
            --authorized-only) REAP_AUTHORIZED_ONLY=1; shift ;;
            --eligible-keys-stdin) REAP_ELIGIBLE_STDIN=1; shift ;;
            --eligible-key)
                _eligible_key="${2:-}"
                case "$_eligible_key" in
                    ""|*[!A-Za-z0-9._-]*) die "--eligible-key must be a safe lane key" ;;
                esac
                REAP_ELIGIBLE="${REAP_ELIGIBLE}${REAP_ELIGIBLE:+$'\n'}${_eligible_key}"
                shift 2
                ;;
            *) die "unknown reap arg: $1" ;;
        esac
    done
    case "$REAP_IDLE" in
        *[!0-9]*|"") die "--idle-seconds must be a non-negative integer" ;;
    esac
    # ur-rp-23: an absent budget stays empty, so an operator run keeps the default window.
    if [ "$REAP_CALLER_BUDGET_GIVEN" -eq 1 ]; then
        case "$REAP_CALLER_BUDGET" in
            *[!0-9]*|"") die "--caller-budget-seconds must be a non-negative integer" ;;
        esac
    fi
    # AGT-08: no key source at all is a caller error. An explicit empty stdin is not:
    # it is the legitimate "authorized, nothing eligible" sweep lane_census sends.
    if [ "$REAP_AUTHORIZED_ONLY" -eq 1 ] && [ -z "$REAP_ELIGIBLE" ] && [ "$REAP_ELIGIBLE_STDIN" -eq 0 ]; then
        die "authorized_only_empty_allowlist: --authorized-only needs --eligible-key or --eligible-keys-stdin"
    fi
    _reap_work_dir="$(mktemp -d "${TMPDIR:-/tmp}/remote-agent-reap.XXXXXX")" || die "cannot create reap allowlist temp dir"
    trap 'rm -rf "$_reap_work_dir"' EXIT
    _reap_authorized="$_reap_work_dir/authorized"
    _reap_present="$_reap_work_dir/present"
    _reap_selected="$_reap_work_dir/selected"
    : >"$_reap_authorized"
    : >"$_reap_present"
    if [ -n "$REAP_ELIGIBLE" ]; then
        printf '%s\n' "$REAP_ELIGIBLE" >"$_reap_authorized"
    fi
    if [ "$REAP_ELIGIBLE_STDIN" -eq 1 ]; then
        cat >>"$_reap_authorized"
    fi
    _eligible_key=
    while IFS= read -r _eligible_key || [ -n "$_eligible_key" ]; do
        case "$_eligible_key" in
            ""|*[!A-Za-z0-9._-]*) die "eligible-key stdin must contain safe lane keys" ;;
        esac
    done <"$_reap_authorized"

    # Name the mode before the observation-only downgrade below rewrites REAP_DRY.
    if [ "$REAP_DRY" -eq 1 ]; then
        reap_mode=dry_run
    elif [ "$REAP_AUTHORIZED_ONLY" -eq 1 ]; then
        reap_mode=authorized_only_empty
        if [ -s "$_reap_authorized" ]; then reap_mode=authorized_only; fi
    elif [ -s "$_reap_authorized" ]; then
        reap_mode=apply
    else
        reap_mode=observation_only
        _reap_degraded_reason=no_allowlist
    fi

    if [ "$REAP_DRY" -eq 0 ] && [ "$REAP_AUTHORIZED_ONLY" -eq 0 ] && [ ! -s "$_reap_authorized" ]; then
        echo "remote_agent: no authoritative eligible-key allowlist; reap is observation-only" >&2
        REAP_DRY=1
    fi
    : >"$_reap_selected"
    if [ -s "$_reap_authorized" ] && { [ "$REAP_DRY" -eq 0 ] || [ "$REAP_AUTHORIZED_ONLY" -eq 1 ]; }; then
        _reap_list_script() {
            cat <<'REAP_LIST_EOF'
set -euo pipefail
agent_root="$1"
case "$agent_root" in
    /*) ROOT="$agent_root" ;;
    *) ROOT="$HOME/$agent_root" ;;
esac
[ -d "$ROOT" ] || exit 0
for _candidate in "$ROOT"/*/; do
    [ -d "${_candidate%/}" ] || continue
    [ -f "${_candidate%/}/.workbay-lane-sandbox" ] || continue
    _candidate_key="${_candidate%/}"
    _candidate_key="${_candidate_key##*/}"
    printf '%s\n' "$_candidate_key" | LC_ALL=C grep -Eq '^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$' && continue
    [ ! -f "${_candidate%/}/.workbay-attempt-evidence" ] || continue
    printf '%s\n' "$_candidate_key"
done
REAP_LIST_EOF
        }
        if ! _reap_present_output=$(_reap_list_script | "${SSH[@]}" bash -s -- "$AGENT_ROOT" "$REAP_IDLE" "$REAP_DRY" "$REAP_AUTHORIZED_ONLY"); then
            die "could not list marked VM sandboxes"
        fi
        if [ -n "$_reap_present_output" ]; then
            printf '%s\n' "$_reap_present_output" >"$_reap_present"
        fi
        _present_key=
        while IFS= read -r _present_key || [ -n "$_present_key" ]; do
            case "$_present_key" in
                ""|*[!A-Za-z0-9._-]*) die "unsafe lane key in VM sandbox listing" ;;
            esac
        done <"$_reap_present"
        LC_ALL=C sort -u "$_reap_authorized" >"$_reap_authorized.sorted"
        LC_ALL=C sort -u "$_reap_present" >"$_reap_present.sorted"
        LC_ALL=C comm -12 "$_reap_authorized.sorted" "$_reap_present.sorted" >"$_reap_selected"
    fi
    _eligible_keys_csv=$(LC_ALL=C tr '\n' ',' <"$_reap_selected")
    _reap_hash_line=
    if command -v shasum >/dev/null 2>&1; then
        _reap_hash_line="$(shasum -a 256 "$0" 2>/dev/null || true)"
    fi
    if [ -z "$_reap_hash_line" ] && command -v sha256sum >/dev/null 2>&1; then
        _reap_hash_line="$(sha256sum "$0" 2>/dev/null || true)"
    fi
    _reap_script_sha256="${_reap_hash_line%% *}"
    case "$_reap_script_sha256" in *[!0-9a-f]*|"") _reap_script_sha256= ;; esac
    if [ "${#_reap_script_sha256}" -ne 64 ]; then _reap_script_sha256=; fi
    # Host-only setting: resolved and checked here, then carried in the metadata
    # field after the ceiling. The VM body never reads the variable itself.
    _reap_outcomes_cap="${WORKBAY_REMOTE_AGENT_REAP_OUTCOMES_MAX_BYTES:-8388608}"
    case "$_reap_outcomes_cap" in
        *[!0-9]*)
            printf 'remote_agent: reap_outcomes_cap_invalid: WORKBAY_REMOTE_AGENT_REAP_OUTCOMES_MAX_BYTES must be a non-negative integer; using 8388608\n' >&2
            _reap_outcomes_cap=8388608
            ;;
    esac
    # The remote mutex serialises the entire check/act pass. Per-lane locks are
    # then held across the final lease/pid re-probe and deletion [CON-11].
    {
        printf '%s\n' "$_probe_trust_remote_sh"
        printf 'eligible_keys=%s\n' "$_eligible_keys_csv"
        printf 'reap_mode=%s\n' "$reap_mode"
        printf 'reap_degraded_reason=%s\n' "$_reap_degraded_reason"
        cat <<'REAP_EOF'
set -euo pipefail
if [ "${1:-}" = "--" ]; then shift; fi
agent_root="$1"
idle_limit="$2"
dry_run="$3"
_reap_arg4="${4:-}"
# ur-rp-23: the caller's wait, as the 5th positional. Digits up to 9 places are kept, and
# anything else is absent. A budget that large never shrinks the window anyway.
_reap_caller_budget="${5:-}"
case "$_reap_caller_budget" in ''|*[!0-9]*|??????????*) _reap_caller_budget= ;; *) _reap_caller_budget=$((10#$_reap_caller_budget)) ;; esac
authorized_only="${_reap_arg4%%:*}"
_reap_scope="${_reap_arg4#*:}"
_legacy_eligible="${_reap_scope%%:*}"
if [ "$_reap_scope" = "$_legacy_eligible" ]; then
    tmp_root="$_reap_scope"
    _reap_remote_metadata="${5:-}"
else
    _reap_scope="${_reap_scope#*:}"
    tmp_root="${_reap_scope%%:*}"
    if [ "$_reap_scope" = "$tmp_root" ]; then
        _reap_remote_metadata="${5:-}"
    else
        _reap_remote_metadata="${_reap_scope#*:}"
    fi
fi
eligible_keys="${eligible_keys:-$_legacy_eligible}"
# The host sends reap_mode and reap_degraded_reason on the prefix lines. An older
# host sends neither, so name the mode from the flags this body was given. The
# prefix lines are untrusted (SEC-01): a value off the allowlist is dropped, the
# mode falls back to the flags, and degraded_reason says so (API-01).
reap_mode="${reap_mode:-}"
reap_degraded_reason="${reap_degraded_reason:-}"
_reap_host_mode_valid() {
    case "$reap_mode" in
        ''|apply|dry_run|authorized_only|authorized_only_empty|observation_only) ;;
        *) return 1 ;;
    esac
    case "$reap_degraded_reason" in
        '') return 0 ;;
        *[!a-z_]*) return 1 ;;
    esac
    [ "${#reap_degraded_reason}" -le 64 ]
}
if ! _reap_host_mode_valid; then
    reap_mode=
    reap_degraded_reason=invalid_host_mode
fi
if [ -z "$reap_mode" ]; then
    if [ "$dry_run" -eq 1 ]; then
        reap_mode=dry_run
    elif [ "$authorized_only" -eq 1 ]; then
        reap_mode=authorized_only_empty
        if [ -n "$eligible_keys" ]; then reap_mode=authorized_only; fi
    else
        reap_mode=apply
    fi
fi
_script_sha256="${_reap_remote_metadata%%:*}"
# Metadata is sha:ceiling[:reap_outcomes_cap]; the cap field is optional.
_reap_metadata_tail="${_reap_remote_metadata#*:}"
scope_overage_ceiling="${_reap_metadata_tail%%:*}"
[ "$_reap_metadata_tail" != "$_reap_remote_metadata" ] || scope_overage_ceiling=21600
case "$scope_overage_ceiling" in ''|*[!0-9]*) scope_overage_ceiling=21600 ;; esac
[ "$scope_overage_ceiling" -gt 0 ] || scope_overage_ceiling=21600
_reap_outcomes_cap="${_reap_metadata_tail#*:}"
[ "$_reap_outcomes_cap" != "$_reap_metadata_tail" ] || _reap_outcomes_cap=
case "$_script_sha256" in
    *[!0-9a-f]*|"") _script_sha256= ;;
esac
if [ "${#_script_sha256}" -ne 64 ]; then
    _script_sha256=
fi
case "$agent_root" in
    /*) ROOT="$agent_root" ;;
    *) ROOT="$HOME/$agent_root" ;;
esac
mkdir -p "$ROOT"

_sandbox_count() {
    _count=0
    for _candidate in "$ROOT"/*/; do
        [ -f "${_candidate%/}/.workbay-lane-sandbox" ] || continue
        _candidate_key="${_candidate%/}"
        _candidate_key="${_candidate_key##*/}"
        printf '%s\n' "$_candidate_key" | LC_ALL=C grep -Eq '^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$' && continue
        [ ! -f "${_candidate%/}/.workbay-attempt-evidence" ] || continue
        _count=$((_count + 1))
    done
    printf '%s' "$_count"
}

# ur-rp-30: every byte in 0x00-0x1f is escaped, so no key can make a receipt invalid JSON.
# \b \f \n \r \t take their short forms and the rest \u00XX. Plain substitutions only: no
# python on the VM path, and the body also runs under macOS bash 3.2 in the tests.
_reap_json_escape() {
    _rje_value="${1:-}"
    _rje_value="${_rje_value//\\/\\\\}"
    _rje_value="${_rje_value//\"/\\\"}"
    _rje_value="${_rje_value//$'\x01'/\\u0001}"
    _rje_value="${_rje_value//$'\x02'/\\u0002}"
    _rje_value="${_rje_value//$'\x03'/\\u0003}"
    _rje_value="${_rje_value//$'\x04'/\\u0004}"
    _rje_value="${_rje_value//$'\x05'/\\u0005}"
    _rje_value="${_rje_value//$'\x06'/\\u0006}"
    _rje_value="${_rje_value//$'\x07'/\\u0007}"
    _rje_value="${_rje_value//$'\b'/\\b}"
    _rje_value="${_rje_value//$'\t'/\\t}"
    _rje_value="${_rje_value//$'\n'/\\n}"
    _rje_value="${_rje_value//$'\x0b'/\\u000b}"
    _rje_value="${_rje_value//$'\f'/\\f}"
    _rje_value="${_rje_value//$'\r'/\\r}"
    _rje_value="${_rje_value//$'\x0e'/\\u000e}"
    _rje_value="${_rje_value//$'\x0f'/\\u000f}"
    _rje_value="${_rje_value//$'\x10'/\\u0010}"
    _rje_value="${_rje_value//$'\x11'/\\u0011}"
    _rje_value="${_rje_value//$'\x12'/\\u0012}"
    _rje_value="${_rje_value//$'\x13'/\\u0013}"
    _rje_value="${_rje_value//$'\x14'/\\u0014}"
    _rje_value="${_rje_value//$'\x15'/\\u0015}"
    _rje_value="${_rje_value//$'\x16'/\\u0016}"
    _rje_value="${_rje_value//$'\x17'/\\u0017}"
    _rje_value="${_rje_value//$'\x18'/\\u0018}"
    _rje_value="${_rje_value//$'\x19'/\\u0019}"
    _rje_value="${_rje_value//$'\x1a'/\\u001a}"
    _rje_value="${_rje_value//$'\x1b'/\\u001b}"
    _rje_value="${_rje_value//$'\x1c'/\\u001c}"
    _rje_value="${_rje_value//$'\x1d'/\\u001d}"
    _rje_value="${_rje_value//$'\x1e'/\\u001e}"
    _rje_value="${_rje_value//$'\x1f'/\\u001f}"
    printf '%s' "$_rje_value"
}

# ur-rp-30: one candidate row, bounded at 50 with candidates_truncated. Keys go through the
# escape above: the VM glob is not safe-key filtered. would_reap and would_free_bytes are not
# touched here; they count every candidate, kept or not.
_reap_add_candidate() {
    local _rac_key="${1:-}" _rac_bytes="${2:-0}" _rac_key_json _rac_row
    case "$_rac_bytes" in ''|*[!0-9]*) _rac_bytes=0 ;; esac
    if [ "$_reap_candidates_count" -ge 50 ]; then
        _reap_candidates_truncated=true
        return 0
    fi
    _rac_key_json="$(_reap_json_escape "$_rac_key")"
    _rac_row="{\"key\":\"$_rac_key_json\",\"bytes\":$_rac_bytes}"
    if [ -n "$_reap_candidates_json" ]; then
        _reap_candidates_json="$_reap_candidates_json,$_rac_row"
    else
        _reap_candidates_json="$_rac_row"
    fi
    _reap_candidates_count=$((_reap_candidates_count + 1))
}

_reap_record_hold() {
    local _rh_key="${1:-}" _rh_reason="${2:-}" _rh_pid="${3:-}" _rh_comm="${4:-}" _rh_uid="${5:-}" _rh_age="${6:-0}"
    local _rh_key_json _rh_reason_json _rh_journal_reason="${7:-${2:-}}" _rh_journal_reason_json
    local _rh_comm_json _rh_row _rh_pid_json _rh_uid_json _rh_outcome
    _reap_hold_journal_ok=1
    _rh_key_json="$(_reap_json_escape "$_rh_key")"
    _rh_reason_json="$(_reap_json_escape "$_rh_reason")"
    _rh_journal_reason_json="$(_reap_json_escape "$_rh_journal_reason")"
    _rh_row="{\"key\":\"$_rh_key_json\",\"reason\":\"$_rh_reason_json\""
    if [ -n "$_rh_pid" ]; then
        case "$_rh_pid" in ''|*[!0-9]*) _rh_pid_json=null ;; *) _rh_pid_json="$_rh_pid" ;; esac
        case "$_rh_uid" in ''|*[!0-9]*) _rh_uid_json=null ;; *) _rh_uid_json="$_rh_uid" ;; esac
        case "$_rh_age" in ''|*[!0-9]*) _rh_age=0 ;; esac
        _rh_comm="$(printf '%s' "$_rh_comm" | LC_ALL=C tr -cd 'A-Za-z0-9_.:-')"
        [ -n "$_rh_comm" ] || _rh_comm=unknown
        _rh_comm_json="$(_reap_json_escape "$_rh_comm")"
        _rh_row="$_rh_row,\"pid\":$_rh_pid_json,\"comm\":\"$_rh_comm_json\",\"uid\":$_rh_uid_json"
        if ! printf '{"key":"%s","outcome":"kept_live_pid","reason":"%s","pid":%s,"comm":"%s","uid":%s,"age":%s,"actor":"maintenance","at":%s}\n' \
            "$_rh_key_json" "$_rh_journal_reason_json" "$_rh_pid_json" "$_rh_comm_json" "$_rh_uid_json" "$_rh_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            _reap_hold_journal_ok=0
            echo "remote_agent: kept_live_pid journal failed for $_rh_key" >&2
        fi
    else
        _rh_outcome=kept_proc_probe_unknown
        case "$_rh_reason" in
            held_lock) _rh_outcome=kept_locked ;;
            held_live_lease) _rh_outcome=kept_live_lease ;;
        esac
        case "$_rh_age" in ''|*[!0-9]*) _rh_age=0 ;; esac
        if ! printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
            "$_rh_key_json" "$_rh_outcome" "$_rh_journal_reason_json" "$_rh_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            _reap_hold_journal_ok=0
            echo "remote_agent: $_rh_outcome journal failed for $_rh_key" >&2
        fi
    fi
    _rh_row="$_rh_row}"
    if [ "$_reap_holds_count" -lt 50 ]; then
        if [ -n "$_reap_holds_json" ]; then
            _reap_holds_json="$_reap_holds_json,$_rh_row"
        else
            _reap_holds_json="$_rh_row"
        fi
        _reap_holds_count=$((_reap_holds_count + 1))
    else
        _reap_holds_truncated=true
    fi
}

_reap_holds_json=
_reap_holds_count=0
_reap_holds_truncated=false
_reap_candidates_json=
_reap_candidates_count=0
_reap_candidates_truncated=false
# reap_degraded_reason is allowlisted above (empty or [a-z_]{1,64}), so it needs no escape here.
_reap_degraded_field=
if [ -n "$reap_degraded_reason" ]; then
    _reap_degraded_field=",\"degraded_reason\":\"$reap_degraded_reason\""
fi
_reap_script_sha_field=
if [ -n "$_script_sha256" ]; then
    _reap_script_sha_field=",\"script_sha256\":\"$_script_sha256\""
fi

# Fixed descriptors, not {var}> automatic allocation. This body normally runs
# on the Linux VM, but test_remote_agent_dispatch_reaper.py stubs ssh with
# `exec bash -s` and executes it under the HOST bash -- 3.2.57 on macOS, which
# has no {var}> and exits 127. Fixed fds work on both. Deliberately NOT fd 9:
# test_remote_agent_lane_lock.py locates the per-lane build lock by the FIRST
# `exec 9>` line in the file, so reusing 9 here would shadow it.
command -v flock >/dev/null 2>&1 || { echo 'remote_agent: reap_flock_unavailable: flock required on the VM; refusing to reap without a lock' >&2; exit 78; }
# ur-rp-23: the VM clock alone stamps the start: host and VM clocks are never compared.
# It comes after the flock preflight so a host without tools still reports 78, not a
# missing `date`; the body before it only parses arguments and defines functions.
_reap_body_start=$(date +%s)
reap_lock_fd=7
exec 7>>"$ROOT/.reap.lock"
if ! flock -n "$reap_lock_fd"; then
    printf 'remote_agent: reap lock held: %s\n' "$ROOT/.reap.lock" >&2
    # ur-rp-30: the same mode and degradation fields as the main receipt (API-01). Nothing
    # was examined, so candidates is empty and not truncated.
    printf '{"reaped":0,"would_reap":0,"skipped_live":0,"skipped_locked":0,"bytes_freed":0,"would_free_bytes":0,"sandbox_count_after":%s,"uv_cache_bytes":0,"tmp_scratch_bytes":0,"tmp_scratch_reaped":0,"would_uv_cache_bytes":0,"would_tmp_scratch_bytes":0,"would_tmp_scratch_reaped":0,"holds":[],"holds_truncated":false,"candidates":[],"candidates_truncated":false,"mode":"%s"%s%s}\n' "$(_sandbox_count)" "$reap_mode" "$_reap_degraded_field" "$_reap_script_sha_field"
    exit 75
fi

# Bound reconciliation evidence under the reap mutex, keeping one generation.
# Append writers do not take this lock; their next >> reopens the current path.
# The host resolved the cap from its environment and passed it in the metadata.
REAP_OUTCOMES_MAX_BYTES="${_reap_outcomes_cap:-8388608}"
_reap_outcomes="$ROOT/.reap-outcomes.jsonl"
if [ -e "$_reap_outcomes" ]; then
    _rotation_failure=
    if ! [ "$REAP_OUTCOMES_MAX_BYTES" -ge 0 ] 2>/dev/null; then
        _rotation_failure='invalid size cap'
    elif ! command -v timeout >/dev/null 2>&1; then
        _rotation_failure='timeout unavailable'
    else
        # Escalate TERM to KILL so a stalled tool cannot monopolise the mutex.
        # Children need no copy of fd 7: the parent holds it through rotation.
        _rotation_rc=0
        _reap_outcomes_bytes=$(timeout -k 1 2 stat -c %s "$_reap_outcomes" 7>&- 2>/dev/null) || _rotation_rc=$?
        # Retry the BSD dialect only on ordinary errors, never on a timeout.
        case "$_rotation_rc" in
            0|124|137) ;;
            *)
                _rotation_rc=0
                _reap_outcomes_bytes=$(timeout -k 1 2 stat -f %z "$_reap_outcomes" 7>&- 2>/dev/null) || _rotation_rc=$?
                ;;
        esac
        if [ "$_rotation_rc" -eq 124 ] || [ "$_rotation_rc" -eq 137 ]; then
            _rotation_failure='stat timed out'
        elif [ "$_rotation_rc" -ne 0 ]; then
            _rotation_failure='stat failed'
        elif ! [ "$_reap_outcomes_bytes" -ge 0 ] 2>/dev/null; then
            _rotation_failure='stat returned an invalid size'
        elif [ "$_reap_outcomes_bytes" -gt "$REAP_OUTCOMES_MAX_BYTES" ]; then
            # mv must replace a file, never move the journal into a directory.
            if [ "$dry_run" -eq 1 ]; then
                printf 'remote_agent: reap_outcomes_rotation_dry_run: would rotate %s > %s\n' "$_reap_outcomes_bytes" "$REAP_OUTCOMES_MAX_BYTES" >&2
            elif [ -d "$_reap_outcomes.1" ]; then
                _rotation_failure='rotated path is a directory'
            else
                _rotation_rc=0
                timeout -k 1 2 mv -f -- "$_reap_outcomes" "$_reap_outcomes.1" 7>&- 2>/dev/null || _rotation_rc=$?
                case "$_rotation_rc" in
                    0) ;;
                    124|137) _rotation_failure='mv timed out' ;;
                    *) _rotation_failure='mv failed' ;;
                esac
            fi
        fi
    fi
    if [ -n "$_rotation_failure" ]; then
        printf 'remote_agent: reap-outcomes rotation failed: %s\n' "$_rotation_failure" >&2
    fi
fi

    # Nested copy for the quoted REAP_EOF remote body. Dispatch's column-0
    # definition remains the occupancy extractor's single source [REF-26].
    _lane_occupant_live() {
        _lk="${1:-}"
        [ -n "$_lk" ] || return 0
        _lf="$ROOT/.lane-live-$_lk"
        [ -f "$_lf" ] || return 1
        _expiry=
        _issued=
        _released=
        while IFS= read -r _lline || [ -n "$_lline" ]; do
            # Substring (not ${var#pfx}): extractors strip # as comments [TEST-04].
            case "$_lline" in
                expiry=*) _expiry="${_lline:7}" ;;
                issued=*) _issued="${_lline:7}" ;;
                released=*) _released="${_lline:9}" ;;
            esac
        done <"$_lf" || return 0
        case "$_expiry" in
            ''|*[!0-9]*) return 0 ;;
        esac
        case "$_issued" in
            ''|*[!0-9]*) return 0 ;;
        esac
        case "$_released" in
            1) return 1 ;;
            ''|0) ;;
            *) return 0 ;;
        esac
        _now=$(date +%s)
        if [ "$_now" -ge "$_expiry" ]; then
            return 1
        fi
        return 0
    }

_maintenance_systemctl() {
    if command -v timeout >/dev/null 2>&1; then
        timeout 10 systemctl --user "$@" 2>/dev/null
    else
        printf '{"outcome":"systemctl_unbounded_skipped","reason":"timeout_unavailable","operation":"%s","at":%s}\n' \
            "${1:-unknown}" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
            echo "remote_agent: systemctl_unbounded_skipped journal failed" >&2
        return 125
    fi
}

_maintenance_lane_scope_live() {
    local _lane_key="${1:-}" _scope_output _n
    local -a _scope_units
    [ -n "$_lane_key" ] || return 0
    _scope_units=(
        "workbay-lane-${_lane_key}.scope"
        "workbay-lane-${_lane_key}-sv.scope"
        "grok-lane-${_lane_key}.scope"
        "grok-lane-${_lane_key}-sv.scope"
    )
    for _n in 1 2 3 4 5 6 7 8 9; do
        _scope_units+=(
            "workbay-lane-${_lane_key}-r${_n}.scope"
            "workbay-lane-${_lane_key}-r${_n}-sv.scope"
            "grok-lane-${_lane_key}-r${_n}.scope"
            "grok-lane-${_lane_key}-r${_n}-sv.scope"
        )
    done
    _scope_output=$(_maintenance_systemctl list-units --type=scope --state=active --plain --no-legend "${_scope_units[@]}") || return 0
    [ -n "$_scope_output" ]
}

_maintenance_reap_evidence() {
    [ "$dry_run" -eq 0 ] || return 0
    local _evidence_dir _evidence_key _lane_key _marker _mtime _age _now
    _now=$(date +%s)
    for _evidence_dir in "$ROOT"/attempt-evidence-*/; do
        _evidence_dir="${_evidence_dir%/}"
        [ -d "$_evidence_dir" ] && [ ! -L "$_evidence_dir" ] || continue
        _evidence_key="${_evidence_dir##*/}"
        [[ "$_evidence_key" =~ ^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$ ]] || continue
        [[ "$_evidence_key" =~ ^attempt-evidence-(.+-[0-9a-f]{8})-[0-9]+-[0-9a-f]{16}$ ]] || continue
        _lane_key="${BASH_REMATCH[1]}"
        [[ "$_lane_key" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}$ ]] || continue
        _marker="$_evidence_dir/.workbay-attempt-evidence"
        [ -f "$_marker" ] && [ ! -L "$_marker" ] || continue
        _mtime=$(_stat_mtime "$_marker" 2>/dev/null) || continue
        case "$_mtime" in ''|*[!0-9]*) continue ;; esac
        _age=$((_now - _mtime))
        [ "$_age" -ge 604800 ] || continue
        _lane_occupant_live "$_lane_key" && continue
        _maintenance_lane_scope_live "$_lane_key" && continue
        exec 9>>"$ROOT/.lane-lock-$_lane_key" || continue
        if ! flock -n 9; then
            exec 9>&-
            continue
        fi
        if [ ! -d "$_evidence_dir" ] || [ -L "$_evidence_dir" ] || [ ! -f "$_marker" ] || [ -L "$_marker" ] || \
            _lane_occupant_live "$_lane_key" || _maintenance_lane_scope_live "$_lane_key"; then
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        _mtime=$(_stat_mtime "$_marker" 2>/dev/null) || {
            flock -u 9 || true
            exec 9>&-
            continue
        }
        case "$_mtime" in ''|*[!0-9]*)
            flock -u 9 || true
            exec 9>&-
            continue
            ;;
        esac
        _now=$(date +%s)
        _age=$((_now - _mtime))
        if [ "$_age" -lt 604800 ]; then
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        if ! printf '{"key":"%s","outcome":"started","reason":"evidence_ttl","age":%s,"actor":"maintenance","at":%s}\n' \
            "$_evidence_key" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            echo "remote_agent: evidence_ttl journal failed for $_evidence_key; preserving evidence" >&2
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        rm -rf -- "$_evidence_dir" 2>/dev/null || true
        if [ ! -e "$_evidence_dir" ]; then
            printf '{"key":"%s","outcome":"reaped","reason":"evidence_ttl","age":%s,"actor":"maintenance","at":%s}\n' \
                "$_evidence_key" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: evidence_ttl completion journal failed for $_evidence_key" >&2
        else
            printf '{"key":"%s","outcome":"incomplete","reason":"evidence_ttl","age":%s,"actor":"maintenance","at":%s}\n' \
                "$_evidence_key" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
        fi
        flock -u 9 || true
        exec 9>&-
    done
}

_maintenance_gc_lease_locks() {
    [ "$dry_run" -eq 0 ] || return 0
    local _lease_lock _lease_key _sandbox
    for _lease_lock in "$ROOT"/.lane-lease-*.lock; do
        [ -f "$_lease_lock" ] && [ ! -L "$_lease_lock" ] || continue
        _lease_key="${_lease_lock##*/.lane-lease-}"
        _lease_key="${_lease_key%.lock}"
        [[ "$_lease_key" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}$ ]] || continue
        _sandbox="$ROOT/$_lease_key"
        [ -e "$_sandbox" ] || [ -L "$_sandbox" ] && continue
        _lane_occupant_live "$_lease_key" && continue
        exec 9>>"$_lease_lock" || continue
        if ! flock -n 9; then
            exec 9>&-
            continue
        fi
        if [ -e "$_sandbox" ] || [ -L "$_sandbox" ] || _lane_occupant_live "$_lease_key"; then
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        if ! printf '{"key":"%s","outcome":"started","reason":"lease_lock_gc","actor":"maintenance","at":%s}\n' \
            "$_lease_key" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            echo "remote_agent: lease_lock_gc journal failed for $_lease_key; preserving lock" >&2
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        rm -f -- "$_lease_lock" 2>/dev/null || true
        if [ ! -e "$_lease_lock" ]; then
            printf '{"key":"%s","outcome":"lease_lock_gc","reason":"lease_lock_gc","actor":"maintenance","at":%s}\n' \
                "$_lease_key" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: lease_lock_gc completion journal failed for $_lease_key" >&2
        fi
        flock -u 9 || true
        exec 9>&-
    done
}

_maintenance_parse_runtime() {
    local _raw="${1:-}" _rest _value _unit _factor
    _SCOPE_RUNTIME_PRESENT=0
    _SCOPE_RUNTIME_SECONDS=0
    case "$_raw" in
        infinity|0) return 0 ;;
        '') return 1 ;;
    esac
    if [[ "$_raw" =~ ^[0-9]+$ ]]; then
        _SCOPE_RUNTIME_SECONDS=$((_raw / 1000000))
        _SCOPE_RUNTIME_PRESENT=1
        return 0
    fi
    _rest="$_raw"
    while [ -n "$_rest" ]; do
        if [[ "$_rest" =~ ^[[:space:]]*([0-9]+)[[:space:]]*(usec|us|msec|ms|sec|s|min|m|h|d|w)(.*)$ ]]; then
            _value="${BASH_REMATCH[1]}"
            _unit="${BASH_REMATCH[2]}"
            _rest="${BASH_REMATCH[3]}"
            case "$_unit" in
                usec|us) _factor=0 ;;
                msec|ms) _factor=0 ;;
                sec|s) _factor=1 ;;
                min|m) _factor=60 ;;
                h) _factor=3600 ;;
                d) _factor=86400 ;;
                w) _factor=604800 ;;
                *) return 1 ;;
            esac
            _SCOPE_RUNTIME_SECONDS=$((_SCOPE_RUNTIME_SECONDS + _value * _factor))
        else
            return 1
        fi
    done
    _SCOPE_RUNTIME_PRESENT=1
    return 0
}

_maintenance_journal_scope_skip() {
    local _unit="${1:-}" _lane_key="${2:-}" _reason="${3:-unknown}" _age="${4:-null}"
    printf '{"unit":"%s","lane_key":"%s","outcome":"skipped_live","reason":"%s","age":%s,"at":%s}\n' \
        "$_unit" "$_lane_key" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
        echo "remote_agent: scope overage skip journal failed for $_unit" >&2
}

_maintenance_journal_scope_metadata_skip() {
    local _unit="${1:-}" _reason="${2:-unknown}"
    printf '{"unit":"%s","outcome":"scope_overage_skipped","reason":"%s","at":%s}\n' \
        "$_unit" "$_reason" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
        echo "remote_agent: scope overage metadata journal failed for $_unit" >&2
}

_maintenance_stop_overage_scopes() {
    [ "$dry_run" -eq 0 ] || return 0
    local _scope_list _now_usec _line _unit _lane_key _enter_usec _runtime_raw _props _age _limit _runtime_log _stop_status _proc_root
    _scope_list=$(_maintenance_systemctl list-units --type=scope --state=active --plain --no-legend 'workbay-lane-*' 'grok-lane-*') || return 0
    _proc_root="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    _now_usec=$(awk '{printf "%.0f", $1 * 1000000}' "$_proc_root/uptime" 2>/dev/null) || return 0
    case "$_now_usec" in ''|*[!0-9]*) return 0 ;; esac
    while IFS= read -r _line || [ -n "$_line" ]; do
        read -r _unit _ <<<"$_line"
        if [[ "$_unit" =~ ^(workbay-lane|grok-lane)-([A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8})(-r[0-9]+)?(-sv)?\.scope$ ]]; then
            _lane_key="${BASH_REMATCH[2]}"
        else
            continue
        fi
        _props=$(_maintenance_systemctl show --no-pager --property=ActiveEnterTimestampMonotonic --property=RuntimeMaxUSec "$_unit") || {
            _maintenance_journal_scope_metadata_skip "$_unit" scope_metadata_probe_failed
            continue
        }
        _enter_usec=$(printf '%s\n' "$_props" | sed -n 's/^ActiveEnterTimestampMonotonic=//p' | head -n1)
        _runtime_raw=$(printf '%s\n' "$_props" | sed -n 's/^RuntimeMaxUSec=//p' | head -n1)
        case "$_enter_usec" in ''|*[!0-9]*)
            _maintenance_journal_scope_metadata_skip "$_unit" active_enter_timestamp_missing_or_malformed
            continue
            ;;
        esac
        if [ -z "$_runtime_raw" ]; then
            _maintenance_journal_scope_metadata_skip "$_unit" runtime_ceiling_missing
            continue
        fi
        if ! _maintenance_parse_runtime "$_runtime_raw"; then
            _maintenance_journal_scope_metadata_skip "$_unit" runtime_ceiling_malformed
            continue
        fi
        if [ "$_SCOPE_RUNTIME_PRESENT" -ne 1 ]; then
            _maintenance_journal_scope_metadata_skip "$_unit" runtime_ceiling_unbounded
            continue
        fi
        _age=$(( _now_usec / 1000000 - _enter_usec / 1000000 ))
        [ "$_age" -ge 0 ] || continue
        _limit=$((_SCOPE_RUNTIME_SECONDS + 600))
        _runtime_log="$_SCOPE_RUNTIME_SECONDS"
        [ "$_age" -gt "$_limit" ] || continue
        # Dispatch claims lane-lock before it accesses the lease. Mirror that
        # order and keep both fences through the liveness recheck and stop.
        # The subshell scopes both descriptors so every exit releases them.
        (
            if ! exec 8>>"$ROOT/.lane-lock-$_lane_key"; then
                _maintenance_journal_scope_skip "$_unit" "$_lane_key" dispatch_lock_unavailable "$_age"
                exit 0
            fi
            if ! flock -n 8; then
                _maintenance_journal_scope_skip "$_unit" "$_lane_key" dispatch_lock_held "$_age"
                exit 0
            fi
            if ! exec 9>>"$ROOT/.lane-lease-$_lane_key.lock"; then
                _maintenance_journal_scope_skip "$_unit" "$_lane_key" lease_lock_unavailable "$_age"
                exit 0
            fi
            if ! flock -n 9; then
                _maintenance_journal_scope_skip "$_unit" "$_lane_key" lease_lock_held "$_age"
                exit 0
            fi
            if _lane_occupant_live "$_lane_key"; then
                _maintenance_journal_scope_skip "$_unit" "$_lane_key" live_lease "$_age"
                exit 0
            fi
            if ! _maintenance_systemctl kill --kill-whom=all --signal=KILL "$_unit"; then
                printf '{"unit":"%s","outcome":"scope_overage_stop_failed","reason":"whole_cgroup_kill_failed","age":%s,"at":%s}\n' \
                    "$_unit" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
                exit 0
            fi
            _stop_status=stopped
            _maintenance_systemctl stop "$_unit" || _stop_status=stop_failed
            printf '{"unit":"%s","outcome":"scope_overage_stopped","reason":"scope_overage_stopped","age":%s,"runtime_max_sec":%s,"scope_limit_sec":%s,"stop_status":"%s","at":%s}\n' \
                "$_unit" "$_age" "$_runtime_log" "$_limit" "$_stop_status" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: scope_overage_stopped journal failed for $_unit" >&2
        )
    done <<<"$_scope_list"
}

_maintenance_lane_key_for_failed_unit() {
    local _unit="${1:-}"
    _MAINTENANCE_FAILED_LANE_KEY=
    if [[ "$_unit" =~ ^(workbay-lane|grok-lane)-([A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8})(-r[0-9]+)?(-sv)?\.scope$ ]]; then
        _MAINTENANCE_FAILED_LANE_KEY="${BASH_REMATCH[2]}"
    elif [[ "$_unit" =~ ^(workbay-lane|grok-lane)-([A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8})-job-[A-Za-z0-9._-]+\.service$ ]]; then
        _MAINTENANCE_FAILED_LANE_KEY="${BASH_REMATCH[2]}"
    elif [[ "$_unit" =~ ^workbay-proof-([A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8})\.(service|scope|target)$ ]]; then
        _MAINTENANCE_FAILED_LANE_KEY="${BASH_REMATCH[1]}"
    else
        return 1
    fi
    return 0
}

_maintenance_reset_failed_units() {
    [ "$dry_run" -eq 0 ] || return 0
    local _failed_units _line _unit _lane_key _skip_reason _reset_status
    _failed_units=$(_maintenance_systemctl list-units --state=failed --plain --no-legend 'workbay-*' 'grok-*' 'workbay-proof-*') || return 0
    while IFS= read -r _line || [ -n "$_line" ]; do
        read -r _unit _ <<<"$_line"
        [[ "$_unit" == workbay-* || "$_unit" == grok-* ]] || continue
        if [[ "$_unit" =~ ^workbay-proof-[0-9a-f]{32}\.(service|scope|target)$ ]]; then
            _reset_status=reset_failed
            if ! _maintenance_systemctl reset-failed "$_unit"; then
                _reset_status=reset_failed_failed
            fi
            if [ "$_reset_status" = reset_failed ]; then
                printf '{"unit":"%s","outcome":"reset_failed_resolved","reason":"stale_failed_proof_unit","at":%s}\n' \
                    "$_unit" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            else
                printf '{"unit":"%s","outcome":"reset_failed_failed","reason":"reset_failed_command_failed","at":%s}\n' \
                    "$_unit" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            fi
            continue
        fi
        if ! _maintenance_lane_key_for_failed_unit "$_unit"; then
            printf '{"unit":"%s","outcome":"reset_failed_skipped_unresolved","reason":"lane_key_unresolved","at":%s}\n' \
                "$_unit" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            continue
        fi
        _lane_key="$_MAINTENANCE_FAILED_LANE_KEY"
        if ! exec 9>>"$ROOT/.lane-lease-$_lane_key.lock"; then
            _skip_reason=lease_lock_unavailable
        elif ! flock -n 9; then
            _skip_reason=lease_lock_held
        elif _lane_occupant_live "$_lane_key"; then
            _skip_reason=live_lease
        elif _maintenance_lane_scope_live "$_lane_key"; then
            _skip_reason=live_sandbox
        else
            _reset_status=reset_failed
            if ! _maintenance_systemctl reset-failed "$_unit"; then
                _reset_status=reset_failed_failed
            fi
            if [ "$_reset_status" = reset_failed ]; then
                printf '{"lane_key":"%s","outcome":"reset_failed_resolved","reason":"stale_failed_unit","units":["%s"],"at":%s}\n' \
                    "$_lane_key" "$_unit" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            else
                printf '{"unit":"%s","lane_key":"%s","outcome":"reset_failed_failed","reason":"reset_failed_command_failed","at":%s}\n' \
                    "$_unit" "$_lane_key" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            fi
            flock -u 9 || true
            exec 9>&-
            continue
        fi
        if [ "$_skip_reason" = lease_lock_held ] || [ "$_skip_reason" = lease_lock_unavailable ]; then
            exec 9>&-
        else
            flock -u 9 || true
            exec 9>&-
        fi
        printf '{"unit":"%s","lane_key":"%s","outcome":"reset_failed_skipped_live","reason":"%s","at":%s}\n' \
            "$_unit" "$_lane_key" "$_skip_reason" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
    done <<<"$_failed_units"
}

_lane_is_merged_marked() {
    _lk="${1:-}"
    [ -n "$_lk" ] || return 1
    _mf="$ROOT/.merged/${_lk}.json"
    [ -s "$_mf" ]
}

_PROC_CWD_SNAPSHOT=
_PROC_CWD_TABLE=
_PROC_CWD_HOLD_TABLE=
_PROC_CWD_SNAPSHOT_READY=0
_PROC_CWD_PROBE_REASON=
_lane_proc_root() {
    printf '%s' "${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
}

_lane_proc_root_available() {
    _pr="$(_lane_proc_root)"
    if [ ! -d "$_pr" ] || [ ! -r "$_pr" ] || [ ! -x "$_pr" ]; then
        _PROC_CWD_PROBE_REASON=proc_root_unavailable
        return 1
    fi
    return 0
}

_lane_cwd_points_at() {
    _target="${1:-}"
    _dir="${2:-}"
    [ -n "$_target" ] && [ -n "$_dir" ] || return 1
    case "$_dir" in
        "$ROOT"|"$ROOT"/*) ;;
        *) return 1 ;;
    esac
    case "$_target" in
        "$_dir"|"$_dir"/*|"$_dir (deleted)"|"$_dir/"*" (deleted)")
            case "$_target" in
                "$ROOT"|"$ROOT"/*|"$ROOT (deleted)"|"$ROOT/"*" (deleted)")
                    return 0
                    ;;
            esac
            ;;
    esac
    return 1
}

_lane_pid_owned_by_self() {
    _pid="${1:-}"
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    [ -n "$_pid" ] && [ -O "$_pr/$_pid" ]
}

_lane_pid_start_identity() {
    _pid="${1:-}"
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    _LANE_PID_START_IDENTITY=
    [ -n "$_pid" ] || return 1
    _stat_line=
    { IFS= read -r _stat_line <"$_pr/$_pid/stat"; } 2>/dev/null || return 1
    if [[ "$_stat_line" =~ \)[[:space:]](.*)$ ]]; then
        _stat_rest="${BASH_REMATCH[1]}"
    else
        return 1
    fi
    _stat_field=0
    for _field in $_stat_rest; do
        _stat_field=$((_stat_field + 1))
        if [ "$_stat_field" -eq 20 ]; then
            _LANE_PID_START_IDENTITY="$_field"
            return 0
        fi
    done
    return 1
}

_lane_pid_still_in_dir() {
    _pid="${1:-}"
    _dir="${2:-}"
    _expected_target="${3:-}"
    _expected_start="${4:-}"
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    [ -d "$_pr/$_pid" ] || return 1
    _lane_pid_owned_by_self "$_pid" || return 1
    if [ -n "$_expected_start" ]; then
        _lane_pid_start_identity "$_pid" || return 1
        [ "${_LANE_PID_START_IDENTITY:-}" = "$_expected_start" ] || return 1
    fi
    _target=$(readlink "$_pr/$_pid/cwd" 2>/dev/null || true)
    if [ -n "$_expected_target" ]; then
        case "$_expected_target" in
            "${ROOT}/"*" (deleted)") ;;
            *) return 1 ;;
        esac
        [ "$_target" = "$_expected_target" ]
        return
    fi
    _lane_cwd_points_at "$_target" "$_dir"
}

_lane_collect_matching_pids() {
    _mode="${1:-}"
    _dir="${2:-}"
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
        _snapshot_proc_cwds || return 1
    fi
    _self="${EUID:-${UID:-}}"
    _pids=""
    _records=""
    while IFS=$'\t' read -r _pid _uid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        case "$_self" in ''|*[!0-9]*) continue ;; esac
        [ "$_uid" = "$_self" ] || continue
        [ -n "$_target" ] || continue
        if [ "$_mode" = "deleted_root" ]; then
            case "$_target" in
                "$ROOT/"*" (deleted)") ;;
                *) continue ;;
            esac
            _records="${_records}${_pid}"$'\t'"${_target}"$'\t'"${_start}"$'\n'
        else
            _lane_cwd_points_at "$_target" "$_dir" || continue
            _pids="${_pids} ${_pid}"
        fi
    done <<< "${_PROC_CWD_TABLE}"
    if [ "$_mode" = "deleted_root" ]; then
        printf '%s' "$_records"
    else
        printf '%s' "$_pids"
    fi
}

_lane_terminate_pid_list() {
    _dir="${1:-}"
    shift
    _pids="$*"
    [ -n "$_pids" ] || return 0
    _poll="${WORKBAY_REMOTE_AGENT_STOP_POLL_SEC:-10}"
    case "$_poll" in ''|*[!0-9]*) _poll=10 ;; esac
    [ "$_poll" -le 60 ] || _poll=60
    local _stop_epoch _stop_seconds
    _stop_epoch="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
    _stop_seconds=
    if [ -n "$_stop_epoch" ]; then
        _stop_seconds=$((SECONDS + _stop_epoch - $(date +%s)))
    fi
    for _pid in $_pids; do
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
            env kill -s TERM "$_pid" 2>/dev/null || true
        fi
    done
    _waited=0
    while [ "$_waited" -lt "$_poll" ]; do
        _still=0
        for _pid in $_pids; do
            if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
                _still=1
                break
            fi
        done
        [ "$_still" -eq 0 ] && break
        sleep 1
        _waited=$((_waited + 1))
    done
    for _pid in $_pids; do
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
            env kill -s KILL "$_pid" 2>/dev/null || true
        fi
    done
    for _pid in $_pids; do
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
            return 1
        fi
    done
    return 0
}

_lane_terminate_deleted_pid_records() {
    _records="${1:-}"
    [ -n "$_records" ] || return 0
    _poll="${WORKBAY_REMOTE_AGENT_STOP_POLL_SEC:-10}"
    case "$_poll" in ''|*[!0-9]*) _poll=10 ;; esac
    [ "$_poll" -le 60 ] || _poll=60
    local _stop_epoch _stop_seconds
    _stop_epoch="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
    _stop_seconds=
    if [ -n "$_stop_epoch" ]; then
        _stop_seconds=$((SECONDS + _stop_epoch - $(date +%s)))
    fi
    while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            env kill -s TERM "$_pid" 2>/dev/null || true
        fi
    done <<< "$_records"
    _waited=0
    while [ "$_waited" -lt "$_poll" ]; do
        _still=0
        while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
            [ -n "$_pid" ] || continue
            if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
                _still=1
                break
            fi
        done <<< "$_records"
        [ "$_still" -eq 0 ] && break
        sleep 1
        _waited=$((_waited + 1))
    done
    while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            env kill -s KILL "$_pid" 2>/dev/null || true
        fi
    done <<< "$_records"
    while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        if [ -n "${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}" ]; then
            if [ "$SECONDS" -ge "$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            return 1
        fi
    done <<< "$_records"
    return 0
}

_lane_stop_processes() {
    _lane_stop_deadline_remaining() {
        local _deadline _now
        _deadline="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
        case "$_deadline" in ''|*[!0-9]*) printf 60; return 0 ;; esac
        _now=$(date +%s)
        if [ "$_now" -ge "$_deadline" ]; then
            _LANE_STOP_REASON=stop_budget_exhausted
            return 1
        fi
        printf '%s' "$((_deadline - _now))"
    }

    _lane_stop_systemctl() {
        local _timeout
        _timeout="$(_lane_stop_deadline_remaining)" || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
        [ "$_timeout" -le 20 ] || _timeout=20
        if command -v timeout >/dev/null 2>&1; then
            timeout "$_timeout" systemctl --user "$@" 2>/dev/null || true
        else
            systemctl --user "$@" 2>/dev/null || true
        fi
        _lane_stop_deadline_remaining >/dev/null || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
    }
    _LANE_STOP_REASON=
    _LANE_STOP_DEADLINE=$(($(date +%s) + 60))
    for _budget in "${reap_deadline:-}" "${_DISPATCH_SWEEP_DEADLINE:-}"; do
        case "$_budget" in ''|*[!0-9]*) continue ;; esac
        [ "$_budget" -ge "$_LANE_STOP_DEADLINE" ] || _LANE_STOP_DEADLINE="$_budget"
    done
    _key="${1:-}"
    _dir="${2:-}"
    [ -n "$_key" ] && [ -n "$_dir" ] || return 1
    case "$_dir" in
        "$ROOT"|"$ROOT"/*) ;;
        *) return 1 ;;
    esac
    _lane_proc_root_available || return 1
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
        _snapshot_proc_cwds || return 1
    fi
    for _unit_prefix in workbay-lane grok-lane; do
        _lane_stop_systemctl stop "${_unit_prefix}-${_key}.scope" || return 1
        _lane_stop_systemctl stop "${_unit_prefix}-${_key}-sv.scope" || return 1
    done
    _job_list="$(_lane_stop_systemctl list-units --plain --no-legend \
        "workbay-lane-${_key}-job-*" "grok-lane-${_key}-job-*" 2>/dev/null)" || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
    if [ -n "$_job_list" ]; then
        _old_ifs="$IFS"
        IFS=$'\n'
        for _job_line in $_job_list; do
            IFS="$_old_ifs"
            _job_unit="${_job_line%% *}"
            [ -n "$_job_unit" ] || continue
            _lane_stop_systemctl stop "$_job_unit" || return 1
        done
        IFS="$_old_ifs"
    fi
    _pids="$(_lane_collect_matching_pids sandbox "$_dir")" || return 1
    # shellcheck disable=SC2086
    _lane_terminate_pid_list "$_dir" $_pids
}

_lane_reap_deleted_cwd_orphans() {
    _lane_proc_root_available || {
        echo "remote_agent: deleted-cwd occupancy probe unknown (${_PROC_CWD_PROBE_REASON:-proc_probe_failed}); orphan cleanup deferred" >&2
        printf '%s' 0
        return 0
    }
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
        _snapshot_proc_cwds || {
            echo "remote_agent: deleted-cwd occupancy probe unknown (${_PROC_CWD_PROBE_REASON:-proc_probe_failed}); orphan cleanup deferred" >&2
            printf '%s' 0
            return 0
        }
    fi
    _records="$(_lane_collect_matching_pids deleted_root)" || {
        echo "remote_agent: deleted-cwd occupancy probe unknown (${_PROC_CWD_PROBE_REASON:-proc_probe_failed}); orphan cleanup deferred" >&2
        printf '%s' 0
        return 0
    }
    _killed=0
    if [ -n "$_records" ]; then
        _before=0
        while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
            [ -n "$_pid" ] || continue
            _before=$((_before + 1))
        done <<< "$_records"
        _lane_terminate_deleted_pid_records "$_records" || true
        _after=0
        _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
        while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
            [ -n "$_pid" ] || continue
            if [ -d "$_pr/$_pid" ]; then
                _after=$((_after + 1))
            fi
        done <<< "$_records"
        _killed=$((_before - _after))
        [ "$_killed" -ge 0 ] || _killed=0
    fi
    printf '%s' "$_killed"
}

_stat_uid() {
    _su_path="${1:-}"
    [ -n "$_su_path" ] || return 1
    _su_value=
    if _su_value="$(stat -c %u -- "$_su_path" 2>/dev/null)"; then
        case "$_su_value" in ''|*[!0-9]*) _su_value= ;; *) printf '%s' "$_su_value"; return 0 ;; esac
    fi
    if _su_value="$(stat -f %u "$_su_path" 2>/dev/null)"; then
        case "$_su_value" in ''|*[!0-9]*) return 1 ;; *) printf '%s' "$_su_value"; return 0 ;; esac
    fi
    return 1
}

_stat_mtime() {
    _sm_path="${1:-}"
    [ -n "$_sm_path" ] || return 1
    _sm_value=
    if _sm_value="$(stat -c %Y -- "$_sm_path" 2>/dev/null)"; then
        case "$_sm_value" in ''|*[!0-9]*) _sm_value= ;; *) printf '%s' "$_sm_value"; return 0 ;; esac
    fi
    if _sm_value="$(stat -f %m "$_sm_path" 2>/dev/null)"; then
        case "$_sm_value" in ''|*[!0-9]*) return 1 ;; *) printf '%s' "$_sm_value"; return 0 ;; esac
    fi
    return 1
}

_du_bytes() {
    [ "$#" -gt 0 ] || return 1
    _db_output="$(du -sb "$@" 2>/dev/null || true)"
    _db_bytes="$(printf '%s\n' "$_db_output" | awk '
        NF == 0 { next }
        $1 !~ /^[0-9]+$/ { invalid = 1; next }
        { sum += $1; count++ }
        END { if (invalid || count == 0) exit 1; printf "%.0f", sum }
    ')" || _db_bytes=
    case "$_db_bytes" in ''|*[!0-9]*)
        _db_output="$(du -sk "$@" 2>/dev/null || true)"
        _db_bytes="$(printf '%s\n' "$_db_output" | awk '
            NF == 0 { next }
            $1 !~ /^[0-9]+$/ { invalid = 1; next }
            { sum += $1; count++ }
            END { if (invalid || count == 0) exit 1; printf "%.0f", sum * 1024 }
        ')" || _db_bytes=
        ;;
    esac
    case "$_db_bytes" in ''|*[!0-9]*) return 1 ;; esac
    printf '%s' "$_db_bytes"
}

_snapshot_proc_cwds() {
    local _pr _pid_dir _pid _uid _target _start _cwd _i _output _batch_ok _batch_misaligned _owned_i _n_dirs _n_uids _n_owned _n_targets
    local _identity_uid _identity_uid_rc _identity_start _identity_state _identity_target _identity_target_rc _target_checked
    local _batch_rc
    local _probe_deadline _probe_seconds _stat_uid_mode _stat_rc _stat_probe _batch_valid
    local _retry_root _retry_rc _stat_line _stat_errors _stat_error_path _live_rows _error_rows _batch_uid_count
    local _output_index _fallback_index _fallback_rc _fallback_timeout _fallback_line _fallback_errors _fallback_pid _fallback_error_pid
    local _uncertain_count _fallback_count _fallback_uid_count
    local -a _dirs _uids _starts _comms _owned_cwds _owned_pids _targets _full_targets _target_read _verified_states
    local -a _uncertain_dirs _uncertain_indices _batch_uids _fallback_dirs _fallback_indices _fallback_uids _fallback_stat_args
    if [ "${_PROC_CWD_RETRY_ACTIVE:-0}" != 1 ]; then
        _PROC_CWD_RETRY_ACTIVE=1
        _PROC_CWD_RETRY_COUNT=0
        _retry_root=1
    else
        _retry_root=0
    fi
    _PROC_CWD_SNAPSHOT=""
    _PROC_CWD_TABLE=""
    _PROC_CWD_HOLD_TABLE=""
    _PROC_CWD_SNAPSHOT_READY=0
    _PROC_CWD_PROBE_REASON=
    _PROC_CWD_EXAMINED=0
    _PROC_CWD_OWN_UID=0
    _PROC_CWD_FOREIGN_UNREADABLE=0
    _PROC_CWD_ROOT_UNREADABLE=0
    _PROC_CWD_VANISHED=0
    _PROC_CWD_ZOMBIE=0
    _PROC_CWD_SESSION_INFRA_UNREADABLE=0
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    _proc_cwd_pid_stat() {
        local _ps_line _ps_pattern _ps_pid _ps_rest _ps_field _ps_field_index
        _PROC_CWD_PID_COMM=unknown
        _PROC_CWD_PID_STATE=unknown
        _PROC_CWD_PID_START=
        { IFS= read -r _ps_line < "$_pr/$1/stat"; } 2>/dev/null || return 1
        _ps_pid="${_ps_line%% *}"
        if [ "$_ps_pid" != "$1" ]; then _PROC_CWD_PID_COMM=unknown; _PROC_CWD_PID_STATE=; _PROC_CWD_PID_START=; _PROC_CWD_PROBE_REASON=identity_mismatch; return 1; fi
        _ps_pattern='^[0-9]+ \((.*)\) ([A-Za-z]) (.*)$'
        [[ "$_ps_line" =~ $_ps_pattern ]] || return 1
        _PROC_CWD_PID_COMM="${BASH_REMATCH[1]}"
        _PROC_CWD_PID_STATE="${BASH_REMATCH[2]}"
        _ps_rest="${BASH_REMATCH[2]} ${BASH_REMATCH[3]}"
        _ps_field_index=0
        for _ps_field in $_ps_rest; do
            _ps_field_index=$((_ps_field_index + 1))
            if [ "$_ps_field_index" -eq 20 ]; then
                _PROC_CWD_PID_START="$_ps_field"
                break
            fi
        done
        case "$_PROC_CWD_PID_START" in ''|*[!0-9]*) return 1 ;; esac
    }
    _proc_cwd_probe_report_failure() {
        local _report_pid="${1:-unknown}" _report_comm=unknown _report_state=unknown
        case "$_report_pid" in
            ''|*[!0-9]*) ;;
            *)
                if _proc_cwd_pid_stat "$_report_pid"; then
                    _report_comm="$_PROC_CWD_PID_COMM"
                    _report_state="$_PROC_CWD_PID_STATE"
                fi
                ;;
        esac
        echo "remote_agent: proc cwd probe failed pid=$_report_pid comm=$_report_comm state=$_report_state reason=${_PROC_CWD_PROBE_REASON:-proc_probe_failed} examined=${_PROC_CWD_EXAMINED} own_uid=${_PROC_CWD_OWN_UID} foreign_uid_unreadable=${_PROC_CWD_FOREIGN_UNREADABLE} root_unreadable=${_PROC_CWD_ROOT_UNREADABLE} session_infra_unreadable=${_PROC_CWD_SESSION_INFRA_UNREADABLE} vanished=${_PROC_CWD_VANISHED} zombie=${_PROC_CWD_ZOMBIE}" >&2
    }
    _proc_cwd_retry_failure() {
        local _retry_deadline _retry_now _retry_left _retry_wait _retry_can_pause=1
        if [ "${_PROC_CWD_RETRY_COUNT:-0}" -lt 2 ]; then
            _retry_deadline="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
            if [ -n "$_retry_deadline" ]; then
                case "$_retry_deadline" in
                    ''|*[!0-9]*) _retry_can_pause=0 ;;
                    *)
                        _retry_now=$(date +%s)
                        _retry_left=$((_retry_deadline - _retry_now))
                        [ "$_retry_left" -gt 1 ] || _retry_can_pause=0
                        ;;
                esac
            fi
            if [ "$_retry_can_pause" -eq 1 ]; then
                _PROC_CWD_RETRY_COUNT=$((_PROC_CWD_RETRY_COUNT + 1))
                _retry_wait="0.0$((5 + RANDOM % 10))"
                sleep "$_retry_wait"
                _snapshot_proc_cwds
                _retry_rc=$?
                if [ "$_retry_root" -eq 1 ]; then
                    unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                fi
                return "$_retry_rc"
            fi
        fi
        if [ "$_retry_root" -eq 1 ]; then
            unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
        fi
        return 1
    }
    _proc_cwd_session_infra() {
        local _sid _stat _comm _rest _state _ppid _arg _user _argc _status _key _ruid _extra
        _sid="$1"
        IFS= read -r _stat < "$_pr/$_sid/stat" || return 1
        local _pattern="^[0-9]+ \\((.*)\\) (.*)$"
        [[ "$_stat" =~ $_pattern ]] || return 1
        _comm="${BASH_REMATCH[1]}"
        _rest="${BASH_REMATCH[2]}"
        read -r _state _ppid _extra <<< "$_rest"
        case "$_ppid" in ""|*[!0-9]*) return 1 ;; esac
        if [ "$_comm" = gpg-agent ]; then
            [ "${2:-}" != systemd ] || return 1
            local _start= _field _field_index=0 _euid _suid _fsuid _cgroup _supervised=0
            for _field in $_rest; do
                _field_index=$((_field_index + 1))
                if [ "$_field_index" -eq 20 ]; then _start="$_field"; break; fi
            done
            case "$_start" in ''|*[!0-9]*) return 1 ;; esac
            _status="$(cat "$_pr/$_sid/status" 2>/dev/null)" || return 1
            _ruid=
            while read -r _key _arg _euid _suid _fsuid _extra; do
                if [ "$_key" = Uid: ]; then
                    [ -z "$_ruid" ] && [ -z "$_extra" ] || return 1
                    _ruid="$_arg"
                    [ "$_ruid" = "$EUID" ] && [ "$_euid" = "$EUID" ] &&
                        [ "$_suid" = "$EUID" ] && [ "$_fsuid" = "$EUID" ] || return 1
                fi
            done <<< "$_status"
            [ "$_ruid" = "$EUID" ] || return 1
            # Preserve trailing newlines so only the exact single cgroup line qualifies.
            _cgroup="$(cat "$_pr/$_sid/cgroup" 2>/dev/null && printf '.')" || return 1
            [ "$_cgroup" = "0::/user.slice/user-$EUID.slice/user@$EUID.service/app.slice/gpg-agent.service"$'\n.' ] || return 1
            [ -r "$_pr/$_sid/cmdline" ] || return 1
            while IFS= read -r -d "" _arg; do
                [ "$_arg" != --supervised ] || _supervised=1
            done < "$_pr/$_sid/cmdline"
            [ "$_supervised" = 1 ] && [ -z "$_arg" ] || return 1
            _proc_cwd_session_infra "$_ppid" systemd || return 1
            _proc_cwd_pid_stat "$_sid" || return 1
            [ "$_PROC_CWD_PID_START" = "$_start" ] && [ "$_PROC_CWD_PID_COMM" = "$_comm" ]
            return $?
        fi
        _user=0
        _argc=0
        [ -r "$_pr/$_sid/cmdline" ] || return 1
        while IFS= read -r -d "" _arg; do
            _argc=$((_argc + 1))
            [ "$_arg" != "--user" ] || _user=1
        done < "$_pr/$_sid/cmdline"
        [ "$_argc" -gt 0 ] || return 1
        _status="$(cat "$_pr/$_ppid/status" 2>/dev/null)" || return 1
        _ruid=
        while read -r _key _arg _extra; do
            [ "$_key" != "Uid:" ] || _ruid="$_arg"
        done <<< "$_status"
        case "$_ruid" in ""|*[!0-9]*) return 1 ;; esac
        if [ "${2:-}" = systemd ] && [ "$_comm" != systemd ]; then
            return 1
        fi
        case "$_comm" in
            systemd) [ "$_user" -eq 1 ] && [ "$_ruid" = 0 ] ;;
            "(sd-pam)") _proc_cwd_session_infra "$_ppid" systemd ;;
            tailscaled|sshd) [ "$_ruid" = 0 ] ;;
            *) return 1 ;;
        esac
    }
    _proc_cwd_readlink() {
        local _left
        if [ -n "$_probe_seconds" ]; then
            _left=$((_probe_seconds - SECONDS))
            [ "$_left" -gt 0 ] || return 124
            if command -v timeout >/dev/null 2>&1; then
                timeout "$_left" readlink "$@"
            else
                readlink "$@"
            fi
        else
            readlink "$@"
        fi
    }
    _proc_cwd_deadline_expired() {
        local _deadline _now
        _deadline="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
        case "$_deadline" in ''|*[!0-9]*) return 1 ;; esac
        _now=$(date +%s)
        if [ "$_now" -ge "$_deadline" ]; then
            _PROC_CWD_PROBE_REASON=deadline
            return 0
        fi
        return 1
    }
    _proc_cwd_identity_uid() {
        local _uid_deadline _uid_timeout
        _uid_deadline="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
        case "$_uid_deadline" in
            '')
                if [ "$_stat_uid_mode" = gnu ]; then
                    stat -c %u -- "$1" 2>/dev/null
                else
                    stat -f %u "$1" 2>/dev/null
                fi
                ;;
            *[!0-9]*) return 124 ;;
            *)
                _uid_timeout=$((_uid_deadline - $(date +%s)))
                [ "$_uid_timeout" -gt 0 ] || return 124
                command -v timeout >/dev/null 2>&1 || return 2
                if [ "$_stat_uid_mode" = gnu ]; then
                    timeout "$_uid_timeout" stat -c %u -- "$1" 2>/dev/null
                else
                    timeout "$_uid_timeout" stat -f %u "$1" 2>/dev/null
                fi
                ;;
        esac
    }
    if ! _lane_proc_root_available; then
        _PROC_CWD_PROBE_REASON=proc_root_unavailable
        _proc_cwd_probe_report_failure unknown
        _proc_cwd_retry_failure
        return $?
    fi
    _probe_deadline="${_LANE_STOP_DEADLINE:-${_DISPATCH_SWEEP_DEADLINE:-${reap_deadline:-}}}"
    _probe_seconds=
    if [ -n "$_probe_deadline" ]; then
        _probe_seconds=$((SECONDS + _probe_deadline - $(date +%s)))
    fi
    _dirs=()
    _n_dirs=0
    for _pid_dir in "$_pr"/[0-9]*; do
        [ -d "$_pid_dir" ] || continue
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _dirs+=("$_pid_dir")
        _n_dirs=$((_n_dirs + 1))
    done
    _starts=()
    _comms=()
    _i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        if _proc_cwd_pid_stat "$_pid"; then
            _starts+=("$_PROC_CWD_PID_START")
            _proc_comm="$(printf '%s' "$_PROC_CWD_PID_COMM" | LC_ALL=C tr -cd 'A-Za-z0-9_.:-')"
            [ -n "$_proc_comm" ] || _proc_comm=unknown
            _comms+=("$_proc_comm")
        elif [ ! -d "$_pid_dir" ] || { [ ! -e "$_pid_dir/stat" ] && [ ! -L "$_pid_dir/stat" ]; }; then
            _starts+=("")
            _comms+=(unknown)
        else
            # UID collection below decides whether this stat failure is
            # foreign and safe to skip or own and therefore fail closed.
            _starts+=("")
            _comms+=(unknown)
        fi
        _i=$((_i + 1))
    done
    _uids=()
    _n_uids=0
    if [ "$_n_dirs" -gt 0 ]; then
        _stat_uid_mode=
        if _stat_probe="$(stat -c %u -- "$_pr" 2>/dev/null)" && [[ "$_stat_probe" =~ ^[0-9]+$ ]]; then
            _stat_uid_mode=gnu
        elif _stat_probe="$(stat -f %u "$_pr" 2>/dev/null)" && [[ "$_stat_probe" =~ ^[0-9]+$ ]]; then
            _stat_uid_mode=bsd
        else
            _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
            _pid=unknown
            if [[ "${_dirs[0]}" =~ /([0-9]+)$ ]]; then
                _pid="${BASH_REMATCH[1]}"
            fi
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        _stat_rc=0
        if [ "$_stat_uid_mode" = gnu ]; then
            _output=$(stat -c %u -- "${_dirs[@]}" 2>&1) || _stat_rc=$?
        else
            _output=$(stat -f %u "${_dirs[@]}" 2>&1) || _stat_rc=$?
        fi
        _uids=()
        _n_uids=0
        _stat_errors=
        while IFS= read -r _stat_line || [ -n "$_stat_line" ]; do
            case "$_stat_line" in
                '' ) ;;
                *[!0-9]*) _stat_errors="${_stat_errors}${_stat_line}"$'\n' ;;
                *)
                    _uids+=("$_stat_line")
                    _n_uids=$((_n_uids + 1))
                    ;;
            esac
        done <<< "$_output"
        _batch_valid=1
        [ "$_stat_rc" -eq 0 ] && [ "$_n_uids" -eq "$_n_dirs" ] && [ -z "$_stat_errors" ] || _batch_valid=0
        if [ "$_batch_valid" -eq 0 ]; then
            # Keep rows named by stat diagnostics and retry only those rows.
            # If a silent/malformed batch cannot identify the missing rows,
            # treat its live rows as uncertain and recover them in 256-PID
            # chunks rather than forking once per process.
            _batch_uids=(${_uids[@]+"${_uids[@]}"})
            _batch_uid_count="$_n_uids"
            _live_rows=0
            _error_rows=0
            for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
                [ -d "$_pid_dir" ] || continue
                _live_rows=$((_live_rows + 1))
                case "$_stat_errors" in
                    *"$_pid_dir'"*|*"$_pid_dir:"*|*"$_pid_dir"$'\n'*) _error_rows=$((_error_rows + 1)) ;;
                esac
            done
            _uids=()
            _uncertain_dirs=()
            _uncertain_indices=()
            _uncertain_count=0
            if { [ "$_error_rows" -gt 0 ] || [ "$_live_rows" -lt "$_n_dirs" ]; } && \
                [ "$_batch_uid_count" -eq "$((_live_rows - _error_rows))" ]; then
                _output_index=0
                _i=0
                for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
                    if [ ! -d "$_pid_dir" ]; then
                        _uids+=("")
                    else
                        case "$_stat_errors" in
                            *"$_pid_dir'"*|*"$_pid_dir:"*|*"$_pid_dir"$'\n'*)
                                _uids+=("")
                                _uncertain_dirs+=("$_pid_dir")
                                _uncertain_indices+=("$_i")
                                _uncertain_count=$((_uncertain_count + 1))
                                ;;
                            *)
                                _uids+=("${_batch_uids[$_output_index]:-}")
                                _output_index=$((_output_index + 1))
                                ;;
                        esac
                    fi
                    _i=$((_i + 1))
                done
            else
                _i=0
                for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
                    _uids+=("")
                    if [ -d "$_pid_dir" ]; then
                        _uncertain_dirs+=("$_pid_dir")
                        _uncertain_indices+=("$_i")
                        _uncertain_count=$((_uncertain_count + 1))
                    fi
                    _i=$((_i + 1))
                done
            fi

            _fallback_index=0
            while [ "$_fallback_index" -lt "$_uncertain_count" ]; do
                if _proc_cwd_deadline_expired; then
                    [ "$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                    return 1
                fi
                _fallback_dirs=()
                _fallback_indices=()
                _fallback_count=0
                while [ "$_fallback_index" -lt "$_uncertain_count" ] && [ "$_fallback_count" -lt 256 ]; do
                    _pid_dir="${_uncertain_dirs[$_fallback_index]}"
                    _i="${_uncertain_indices[$_fallback_index]}"
                    if [ -d "$_pid_dir" ]; then
                        _fallback_dirs+=("$_pid_dir")
                        _fallback_indices+=("$_i")
                        _fallback_count=$((_fallback_count + 1))
                    else
                        _uids[$_i]=
                    fi
                    _fallback_index=$((_fallback_index + 1))
                done
                [ "$_fallback_count" -gt 0 ] || continue
                _fallback_rc=0
                _fallback_timeout=
                case "$_probe_deadline" in
                    ''|*[!0-9]*) ;;
                    *)
                        _fallback_timeout=$((_probe_deadline - $(date +%s)))
                        [ "$_fallback_timeout" -gt 1 ] || _fallback_timeout=1
                        ;;
                esac
                if [ "$_stat_uid_mode" = gnu ]; then
                    _fallback_stat_args=(-c %u --)
                else
                    _fallback_stat_args=(-f %u)
                fi
                if [ -n "$_fallback_timeout" ]; then
                    if ! command -v timeout >/dev/null 2>&1; then
                        _PROC_CWD_PROBE_REASON=deadline
                        [ "$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                        return 1
                    fi
                    _output=$(timeout "$_fallback_timeout" stat ${_fallback_stat_args[@]+"${_fallback_stat_args[@]}"} ${_fallback_dirs[@]+"${_fallback_dirs[@]}"} 2>&1) || _fallback_rc=$?
                else
                    _output=$(stat ${_fallback_stat_args[@]+"${_fallback_stat_args[@]}"} ${_fallback_dirs[@]+"${_fallback_dirs[@]}"} 2>&1) || _fallback_rc=$?
                fi
                if [ "$_fallback_rc" -eq 124 ] || [ "$_fallback_rc" -eq 137 ]; then
                    _PROC_CWD_PROBE_REASON=deadline
                    [ "$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                    return 1
                fi
                _fallback_uids=()
                _fallback_uid_count=0
                _fallback_errors=
                while IFS= read -r _fallback_line || [ -n "$_fallback_line" ]; do
                    case "$_fallback_line" in
                        '' ) ;;
                        *[!0-9]*) _fallback_errors="${_fallback_errors}${_fallback_line}"$'\n' ;;
                        *)
                            _fallback_uids+=("$_fallback_line")
                            _fallback_uid_count=$((_fallback_uid_count + 1))
                            ;;
                    esac
                done <<< "$_output"
                _live_rows=0
                _fallback_pid=unknown
                _fallback_error_pid=unknown
                for _pid_dir in ${_fallback_dirs[@]+"${_fallback_dirs[@]}"}; do
                    if [ -d "$_pid_dir" ]; then
                        _live_rows=$((_live_rows + 1))
                        if [[ "$_pid_dir" =~ /([0-9]+)$ ]]; then
                            _fallback_pid="${BASH_REMATCH[1]}"
                            case "$_fallback_errors" in
                                *"$_pid_dir'"*|*"$_pid_dir:"*|*"$_pid_dir"$'\n'*) _fallback_error_pid="${BASH_REMATCH[1]}" ;;
                            esac
                        fi
                    fi
                done
                [ "$_fallback_error_pid" = unknown ] || _fallback_pid="$_fallback_error_pid"
                if [ "$_fallback_rc" -eq 0 ] && [ "$_fallback_uid_count" -eq "$_fallback_count" ] && \
                    [ -z "$_fallback_errors" ]; then
                    _i=0
                    for _pid_dir in ${_fallback_dirs[@]+"${_fallback_dirs[@]}"}; do
                        _uids[${_fallback_indices[$_i]}]="${_fallback_uids[$_i]}"
                        _i=$((_i + 1))
                    done
                elif [ "$_live_rows" -eq 0 ] && [ "$_fallback_uid_count" -eq 0 ]; then
                    for _i in ${_fallback_indices[@]+"${_fallback_indices[@]}"}; do _uids[$_i]=; done
                else
                    _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                    _proc_cwd_probe_report_failure "$_fallback_pid"
                    [ "$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                    return 1
                fi
                if _proc_cwd_deadline_expired; then
                    [ "$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                    return 1
                fi
            done
        fi
    fi
    _owned_cwds=()
    _owned_pids=()
    _n_owned=0
    _i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        _uid="${_uids[$_i]:-}"
        _i=$((_i + 1))
        [ -d "$_pid_dir" ] || continue
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        case "$_uid" in ''|*[!0-9]*)
            [ -d "$_pid_dir" ] || continue
            _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
            _pid=unknown
            if [[ "$_pid_dir" =~ /([0-9]+)$ ]]; then
                _pid="${BASH_REMATCH[1]}"
            fi
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
            ;;
        esac
        if [ "$_uid" = "${EUID:-${UID:-}}" ]; then
            if [ -z "${_starts[$((_i - 1))]:-}" ]; then
                _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                _proc_cwd_probe_report_failure "$_pid"
                _proc_cwd_retry_failure
                return $?
            fi
            if [ ! -O "$_pid_dir" ]; then
                [ -d "$_pid_dir" ] || continue
                _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                _pid=unknown
                if [[ "$_pid_dir" =~ /([0-9]+)$ ]]; then
                    _pid="${BASH_REMATCH[1]}"
                fi
            _proc_cwd_probe_report_failure "$_pid"
                _proc_cwd_retry_failure
                return $?
            fi
            if [ -d "$_pid_dir" ]; then
                _cwd="$_pid_dir/cwd"
                if [ -x "$_pid_dir" ] && [ ! -e "$_cwd" ] && [ ! -L "$_cwd" ]; then
                    continue
                fi
                if _proc_cwd_pid_stat "$_pid" && { [ "$_PROC_CWD_PID_STATE" = Z ] || [ "$_PROC_CWD_PID_STATE" = X ]; }; then
                    continue
                fi
                [ -d "$_pid_dir" ] || continue
                _owned_cwds+=("$_cwd")
                _owned_pids+=("$_pid")
                _n_owned=$((_n_owned + 1))
            fi
        fi
    done
    _targets=()
    _n_targets=0
    _batch_ok=0
    _batch_misaligned=0
    if [ "$_n_owned" -gt 0 ]; then
        _batch_rc=0
        _output=$(_proc_cwd_readlink "${_owned_cwds[@]}" 2>/dev/null) || _batch_rc=$?
        if [ "$_batch_rc" -eq 0 ]; then
            while IFS= read -r _target || [ -n "$_target" ]; do
                _targets+=("$_target")
                _n_targets=$((_n_targets + 1))
            done <<< "$_output"
            if [ "$_n_targets" -ne "$_n_owned" ]; then
                _batch_misaligned=1
            else
                _batch_ok=1
            fi
        fi
    fi
    _full_targets=()
    _target_read=()
    _verified_states=()
    _i=0
    _owned_i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        _uid="${_uids[$_i]:-}"
        _target=
        _target_checked=0
        if [ "$_batch_ok" -eq 1 ] && [ "$_uid" = "${EUID:-${UID:-}}" ] && \
            [ "${_owned_pids[$_owned_i]:-}" = "$_pid" ]; then
            _target="${_targets[$_owned_i]:-}"
            _target_checked=1
            _owned_i=$((_owned_i + 1))
        elif [ "$_uid" = "${EUID:-${UID:-}}" ] && \
            [ "${_owned_pids[$_owned_i]:-}" = "$_pid" ]; then
            _owned_i=$((_owned_i + 1))
        fi
        _full_targets+=("$_target")
        _target_read+=("$_target_checked")
        _verified_states+=("")
        _i=$((_i + 1))
    done

    # When the batch failed, collect individual cwd values before the single
    # identity verification pass so those reads are bound to the same snapshot.
    _i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        _uid="${_uids[$_i]:-}"
        if [ "$_uid" = "${EUID:-${UID:-}}" ] && [ "${_target_read[$_i]:-0}" != 1 ]; then
            _cwd="$_pid_dir/cwd"
            if [ ! -d "$_pid_dir" ]; then
                _target_read[$_i]=1
            elif _proc_cwd_pid_stat "$_pid" && { [ "$_PROC_CWD_PID_STATE" = Z ] || [ "$_PROC_CWD_PID_STATE" = X ]; }; then
                _target_read[$_i]=1
            elif [ -x "$_pid_dir" ] && [ ! -e "$_cwd" ] && [ ! -L "$_cwd" ]; then
                _target_read[$_i]=1
            else
                _identity_target_rc=0
                _identity_target=$(_proc_cwd_readlink "$_cwd" 2>/dev/null) || _identity_target_rc=$?
                if [ "$_identity_target_rc" -eq 0 ]; then
                    _full_targets[$_i]="$_identity_target"
                    _target_read[$_i]=1
                else
                    _target_read[$_i]=2
                fi
            fi
        fi
        _i=$((_i + 1))
    done

    # Bind both batched results to the process identity seen at enumeration.
    # A reused PID discards both rows and is sampled once as a new process.
    _i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        if [ ! -d "$_pid_dir" ]; then
            _uids[$_i]=
            _full_targets[$_i]=
            _target_read[$_i]=1
            _verified_states[$_i]=vanished
            _i=$((_i + 1))
            continue
        fi
        if [ "${_uids[$_i]:-}" != "${EUID:-${UID:-}}" ] && [ -z "${_starts[$_i]:-}" ]; then
            # A failed initial stat leaves no identity to bind the batched UID
            # to. Re-read both owner and start time before trusting it as foreign.
            if ! _proc_cwd_pid_stat "$_pid"; then
                if [ ! -d "$_pid_dir" ]; then
                    _uids[$_i]=
                    _full_targets[$_i]=
                    _target_read[$_i]=1
                    _verified_states[$_i]=vanished
                    _i=$((_i + 1))
                    continue
                fi
                _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                _proc_cwd_probe_report_failure "$_pid"
                _proc_cwd_retry_failure
                return $?
            fi
            _identity_start="$_PROC_CWD_PID_START"
            _identity_state="$_PROC_CWD_PID_STATE"
            _identity_uid_rc=0
            _identity_uid=$(_proc_cwd_identity_uid "$_pid_dir") || _identity_uid_rc=$?
            if [ "$_identity_uid_rc" -eq 124 ] || [ "$_identity_uid_rc" -eq 137 ]; then
                _PROC_CWD_PROBE_REASON=deadline
                unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                return 1
            fi
            case "$_identity_uid" in ''|*[!0-9]*) _identity_uid_rc=1 ;; esac
            if [ "$_identity_uid_rc" -ne 0 ]; then
                if [ ! -d "$_pid_dir" ]; then
                    _uids[$_i]=
                    _full_targets[$_i]=
                    _target_read[$_i]=1
                    _verified_states[$_i]=vanished
                    _i=$((_i + 1))
                    continue
                fi
                _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                _proc_cwd_probe_report_failure "$_pid"
                _proc_cwd_retry_failure
                return $?
            fi
            _uids[$_i]="$_identity_uid"
            if [ "$_identity_uid" != "${EUID:-${UID:-}}" ]; then
                if ! _proc_cwd_pid_stat "$_pid"; then
                    if [ ! -d "$_pid_dir" ]; then
                        _uids[$_i]=
                        _full_targets[$_i]=
                        _target_read[$_i]=1
                        _verified_states[$_i]=vanished
                        _i=$((_i + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "$_pid"
                    _proc_cwd_retry_failure
                    return $?
                fi
                if [ "$_PROC_CWD_PID_START" != "$_identity_start" ]; then
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "$_pid"
                    _proc_cwd_retry_failure
                    return $?
                fi
                _starts[$_i]="$_identity_start"
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]="$_identity_state"
                _i=$((_i + 1))
                continue
            fi
        fi
        if ! _proc_cwd_pid_stat "$_pid"; then
            if [ ! -d "$_pid_dir" ]; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]=vanished
                _i=$((_i + 1))
                continue
            fi
            _cwd="$_pid_dir/cwd"
            if [ -x "$_pid_dir" ] && [ ! -e "$_cwd" ] && [ ! -L "$_cwd" ]; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]=vanished
                _i=$((_i + 1))
                continue
            fi
            case "${_uids[$_i]:-}" in
                ''|*[!0-9]*) ;;
                *)
                    if [ "${_uids[$_i]}" != "${EUID:-${UID:-}}" ]; then
                        _full_targets[$_i]=
                        _target_read[$_i]=1
                        _verified_states[$_i]=foreign_unverified
                        _i=$((_i + 1))
                        continue
                    fi
                    ;;
            esac
            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        _identity_start="$_PROC_CWD_PID_START"
        _identity_state="$_PROC_CWD_PID_STATE"
        if [ -n "${_starts[$_i]:-}" ] && [ "$_identity_start" = "${_starts[$_i]}" ]; then
            _verified_states[$_i]="$_identity_state"
            _i=$((_i + 1))
            continue
        fi

        _identity_uid_rc=0
        _identity_uid=$(_proc_cwd_identity_uid "$_pid_dir") || _identity_uid_rc=$?
        if [ "$_identity_uid_rc" -eq 124 ] || [ "$_identity_uid_rc" -eq 137 ]; then
            _PROC_CWD_PROBE_REASON=deadline
            unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
            return 1
        fi
        case "$_identity_uid" in ''|*[!0-9]*) _identity_uid_rc=1 ;; esac
        if [ "$_identity_uid_rc" -ne 0 ]; then
            if [ ! -d "$_pid_dir" ]; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]=vanished
                _i=$((_i + 1))
                continue
            fi
            if _proc_cwd_pid_stat "$_pid" && { [ "$_PROC_CWD_PID_STATE" = Z ] || [ "$_PROC_CWD_PID_STATE" = X ]; }; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]="$_PROC_CWD_PID_STATE"
                _i=$((_i + 1))
                continue
            fi
            _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi

        _cwd="$_pid_dir/cwd"
        _identity_target_rc=0
        _identity_target=$(_proc_cwd_readlink "$_cwd" 2>/dev/null) || _identity_target_rc=$?
        if [ "$_identity_target_rc" -ne 0 ]; then
            if [ ! -d "$_pid_dir" ]; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]=vanished
                _i=$((_i + 1))
                continue
            fi
            if _proc_cwd_pid_stat "$_pid" && { [ "$_PROC_CWD_PID_STATE" = Z ] || [ "$_PROC_CWD_PID_STATE" = X ]; }; then
                _uids[$_i]="$_identity_uid"
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]="$_PROC_CWD_PID_STATE"
                _i=$((_i + 1))
                continue
            fi
            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        if ! _proc_cwd_pid_stat "$_pid"; then
            if [ ! -d "$_pid_dir" ]; then
                _uids[$_i]=
                _full_targets[$_i]=
                _target_read[$_i]=1
                _verified_states[$_i]=vanished
                _i=$((_i + 1))
                continue
            fi
            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        if [ "$_PROC_CWD_PID_START" != "$_identity_start" ]; then
            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        _starts[$_i]="$_identity_start"
        _uids[$_i]="$_identity_uid"
        _full_targets[$_i]="$_identity_target"
        _target_read[$_i]=1
        _verified_states[$_i]="$_PROC_CWD_PID_STATE"
        _i=$((_i + 1))
    done

    if [ "$_batch_misaligned" -eq 1 ]; then
        _PROC_CWD_PROBE_REASON=batch_misaligned
        _pid="${_owned_pids[0]:-unknown}"
        _proc_cwd_probe_report_failure "$_pid"
        _proc_cwd_retry_failure
        return $?
    fi

    _i=0
    for _pid_dir in ${_dirs[@]+"${_dirs[@]}"}; do
        [[ "$_pid_dir" =~ /([0-9]+)$ ]] || continue
        _pid="${BASH_REMATCH[1]}"
        _PROC_CWD_EXAMINED=$((_PROC_CWD_EXAMINED + 1))
        _uid="${_uids[$_i]:-}"
        _target="${_full_targets[$_i]:-}"
        _start="${_starts[$_i]:-}"
        _identity_state="${_verified_states[$_i]:-}"
        _i=$((_i + 1))
        if [ "$_identity_state" = vanished ]; then
            _PROC_CWD_VANISHED=$((_PROC_CWD_VANISHED + 1))
            continue
        fi
        if [ "$_identity_state" = Z ] || [ "$_identity_state" = X ]; then
            _PROC_CWD_VANISHED=$((_PROC_CWD_VANISHED + 1))
            _PROC_CWD_ZOMBIE=$((_PROC_CWD_ZOMBIE + 1))
            continue
        fi
        if [ ! -d "$_pid_dir" ]; then
            _PROC_CWD_VANISHED=$((_PROC_CWD_VANISHED + 1))
            continue
        fi
        case "$_uid" in ''|*[!0-9]*)
            [ -d "$_pid_dir" ] || {
                _PROC_CWD_VANISHED=$((_PROC_CWD_VANISHED + 1))
                continue
            }
            _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
            ;;
        esac
        if [ "$_uid" != "${EUID:-${UID:-}}" ]; then
            if [ "$_uid" = 0 ]; then
                _PROC_CWD_ROOT_UNREADABLE=$((_PROC_CWD_ROOT_UNREADABLE + 1))
            else
                _PROC_CWD_FOREIGN_UNREADABLE=$((_PROC_CWD_FOREIGN_UNREADABLE + 1))
            fi
            continue
        fi
        _PROC_CWD_OWN_UID=$((_PROC_CWD_OWN_UID + 1))
        _cwd="$_pid_dir/cwd"
        if [ -x "$_pid_dir" ] && [ ! -e "$_cwd" ] && [ ! -L "$_cwd" ]; then
            _PROC_CWD_VANISHED=$((_PROC_CWD_VANISHED + 1))
            continue
        fi
        if [ "${_target_read[$((_i - 1))]:-0}" != 1 ]; then
            if [ "${_target_read[$((_i - 1))]:-0}" = 2 ] && _proc_cwd_session_infra "$_pid"; then
                _PROC_CWD_SESSION_INFRA_UNREADABLE=$((_PROC_CWD_SESSION_INFRA_UNREADABLE + 1))
                continue
            fi
            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
            _proc_cwd_probe_report_failure "$_pid"
            _proc_cwd_retry_failure
            return $?
        fi
        _PROC_CWD_SNAPSHOT="${_PROC_CWD_SNAPSHOT}${_target}"$'\n'
        _PROC_CWD_TABLE="${_PROC_CWD_TABLE}${_pid}"$'\t'"${_uid}"$'\t'"${_target}"$'\t'"${_start}"$'\n'
        _PROC_CWD_HOLD_TABLE="${_PROC_CWD_HOLD_TABLE}${_pid}"$'\t'"${_uid}"$'\t'"${_start}"$'\t'"${_comms[$((_i - 1))]:-unknown}"$'\n'
    done
    if ! _lane_proc_root_available; then
        _PROC_CWD_PROBE_REASON=proc_root_unavailable
        _proc_cwd_probe_report_failure unknown
        _proc_cwd_retry_failure
        return $?
    fi
    echo "remote_agent: proc cwd examined=${_PROC_CWD_EXAMINED} own_uid=${_PROC_CWD_OWN_UID} foreign_uid_unreadable=${_PROC_CWD_FOREIGN_UNREADABLE} root_unreadable=${_PROC_CWD_ROOT_UNREADABLE} session_infra_unreadable=${_PROC_CWD_SESSION_INFRA_UNREADABLE} vanished=${_PROC_CWD_VANISHED} zombie=${_PROC_CWD_ZOMBIE}" >&2
    _PROC_CWD_SNAPSHOT_READY=1
    if [ "$_retry_root" -eq 1 ]; then
        unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
    fi
    return 0
}
_pid_in_sandbox_live() {
    _sd="${1:-}"
    [ -n "$_sd" ] || return 0
    _PROC_CWD_PROBE_REASON=
    _PROC_CWD_MATCHED_PID=
    _PROC_CWD_MATCHED_UID=
    _PROC_CWD_MATCHED_COMM=
    _snapshot_proc_cwds || return 0
    while IFS=$'\t' read -r _pid _uid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_target" ] || continue
        case "$_target" in
            "$_sd"|"$_sd"/*|"$_sd (deleted)"|"$_sd/"*" (deleted)")
                _reap_set_matched_process "$_pid" "$_uid" "$_start"
                return 0
                ;;
        esac
    done <<< "${_PROC_CWD_TABLE}"
    if ! _lane_proc_root_available; then
        return 0
    fi
    return 1
}

_reap_set_matched_process() {
    local _rmp_pid="${1:-}" _rmp_uid="${2:-}" _rmp_start="${3:-}"
    local _meta_pid _meta_uid _meta_start _meta_comm
    _PROC_CWD_MATCHED_PID="$_rmp_pid"
    _PROC_CWD_MATCHED_UID="$_rmp_uid"
    _PROC_CWD_MATCHED_COMM=
    while IFS=$'\t' read -r _meta_pid _meta_uid _meta_start _meta_comm || [ -n "${_meta_pid:-}" ]; do
        if [ "$_meta_pid" = "$_rmp_pid" ] && [ "$_meta_uid" = "$_rmp_uid" ] && [ "$_meta_start" = "$_rmp_start" ]; then
            _PROC_CWD_MATCHED_COMM="$_meta_comm"
            _PROC_CWD_PROBE_REASON=live_pid
            return 0
        fi
    done <<< "$_PROC_CWD_HOLD_TABLE"
    _PROC_CWD_MATCHED_PID=
    _PROC_CWD_MATCHED_UID=
    _PROC_CWD_PROBE_REASON=proc_probe_unknown:pid_identity_changed
    return 1
}

_pid_in_sandbox() {
    _sandbox="$1"
    [ -n "$_sandbox" ] || return 1
    _PROC_CWD_PROBE_REASON=
    _PROC_CWD_MATCHED_PID=
    _PROC_CWD_MATCHED_UID=
    _PROC_CWD_MATCHED_COMM=
    _lane_proc_root_available || return 0
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -eq 1 ]; then
        while IFS=$'\t' read -r _pid _uid _target _start || [ -n "${_pid:-}" ]; do
            [ -n "$_target" ] || continue
            case "$_target" in
                "$_sandbox"|"$_sandbox"/*|"$_sandbox (deleted)"|"$_sandbox/"*" (deleted)")
                    _reap_set_matched_process "$_pid" "$_uid" "$_start"
                    return 0
                    ;;
            esac
        done <<< "$_PROC_CWD_TABLE"
        _lane_proc_root_available || return 0
        return 1
    fi
    _pid_in_sandbox_live "$_sandbox"
}

_disk_used_pct() {
    _path="${1:-.}"
    _pct=$(df -P "$_path" 2>/dev/null | awk 'NR==2 { gsub(/%/,"",$5); print $5+0 }')
    case "$_pct" in
        ''|*[!0-9]*) printf '%s' 0 ;;
        *) printf '%s' "$_pct" ;;
    esac
}

reaped=0
would_reap=0
skipped_live=0
skipped_locked=0
bytes_freed=0
would_free_bytes=0
uv_cache_bytes=0
tmp_scratch_bytes=0
tmp_scratch_reaped=0
would_uv_cache_bytes=0
would_tmp_scratch_bytes=0
would_tmp_scratch_reaped=0
# VMREAP-TMP-SCRATCH-UNSWEPT-01: a zero in a reclaim receipt must distinguish
# "examined N candidates, none eligible" from "never examined anything". Every
# rejection path in the /tmp scratch loop below was a bare `continue`, so a
# reaper whose naming gate matched nothing that exists on disk reported a clean
# run forever. These buckets are the receipt's explanation of its own zero, and
# they must BALANCE: candidates_seen == sum(skipped_*) + would_tmp_scratch_reaped.
tmp_scratch_candidates_seen=0
tmp_scratch_scan_truncated=0
_tmp_scratch_scan_seen=0
tmp_scratch_skipped_unnamed=0
tmp_scratch_skipped_no_owner=0
tmp_scratch_skipped_live=0
tmp_scratch_skipped_fresh=0
tmp_scratch_skipped_unmeasurable=0
tmp_scratch_skipped_foreign_owner=0
# ur-rp-23: the bytes behind each skipped bucket. The loop above only RECORDS the
# path of each skipped tree (see _scratch_skip); _tmp_account_skipped measures them
# after the reclaim loop, on its own deadline (COST-06). tmp_unowned_bytes is the
# headline and holds ONLY the two own-uid buckets with no accountable producer:
# fresh, live and foreign_owner mean different things and are never summed in
# (finding 12167).
tmp_scratch_skipped_unnamed_bytes=0
tmp_scratch_skipped_no_owner_bytes=0
tmp_scratch_skipped_fresh_bytes=0
tmp_scratch_skipped_live_bytes=0
tmp_scratch_skipped_foreign_owner_bytes=0
tmp_unowned_bytes=0
tmp_skipped_bytes_unmeasured=0
tmp_skipped_bytes_partial=0
tmp_skipped_bytes_truncated=0
_tmp_skip_unnamed=()
_tmp_skip_no_owner=()
_tmp_skip_fresh=()
_tmp_skip_live=()
_tmp_skip_foreign_owner=()
now=$(date +%s)
reap_budget="${REAP_BUDGET_SEC:-90}"
case "$reap_budget" in *[!0-9]*|"") reap_budget=90 ;; esac
reap_deadline=$((now + reap_budget))
_reap_probe_unknown_reason=
_reap_probe_unknown_rows=0
_reap_probe_budget_deferred=0
_pressure_pct="${WORKBAY_REMOTE_AGENT_DISK_PRESSURE_PCT:-85}"
case "$_pressure_pct" in *[!0-9]*|"") _pressure_pct=85 ;; esac
_used_pct=$(_disk_used_pct "$ROOT")
pressured=0
if [ "$_used_pct" -ge "$_pressure_pct" ]; then
    pressured=1
fi
# SWEEP_MIN_INTERVAL_SEC debounces the DISPATCH-LOCAL sweep, which runs
# implicitly on every lane launch and must not re-scan the whole root each
# time. A standalone `reap` is itself the request for a pass, so it is never
# debounced: honouring the interval here made an explicit --idle-seconds /
# --eligible-key / --dry-run invocation emit a stale all-zero JSON body on
# stdout whenever any pass had run in the preceding interval, with the reason
# visible only on stderr. The caller reads "would_reap": 0 and concludes there
# is nothing to reclaim.
if ! _snapshot_proc_cwds; then
    _reap_probe_unknown_reason="proc_probe_unknown:${_PROC_CWD_PROBE_REASON:-proc_probe_failed}"
    echo "remote_agent: explicit reap occupancy probe unknown (${_PROC_CWD_PROBE_REASON:-proc_probe_failed}); sandbox candidates will be retained" >&2
fi
# ur-rp-23: copy the reap-START snapshot now. _pid_in_sandbox_live re-runs
# _snapshot_proc_cwds whenever the snapshot is not ready, and every run zeroes the
# _PROC_CWD_* counters, so reading them later would report a different pass.
# unreadable = foreign-uid + root-owned pids the probe cannot read. Vanished pids
# (gone) and session infrastructure (identified as non-lane) are left out on
# purpose: neither is a blind spot.
_reap_liveness_examined="${_PROC_CWD_EXAMINED:-0}"
_reap_liveness_unreadable=$((${_PROC_CWD_FOREIGN_UNREADABLE:-0} + ${_PROC_CWD_ROOT_UNREADABLE:-0}))
# Absent, not a sentinel, when the snapshot failed or saw no pid (CARD-03).
_reap_liveness_coverage_field=
if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -eq 1 ] && [ "$_reap_liveness_examined" -gt 0 ]; then
    _reap_liveness_coverage_field=",\"liveness_coverage_pct\":$(( (_reap_liveness_examined - _reap_liveness_unreadable) * 100 / _reap_liveness_examined ))"
fi
candidates=0
_merged_keys=""
skipped_not_allowlisted=0
_unmerged_lines=""
_glob_keys=""
for _sd in "$ROOT"/*/; do
    _sd="${_sd%/}"
    printf '%s\n' "${_sd##*/}" | LC_ALL=C grep -Eq '^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$' && continue
    [ ! -f "$_sd/.workbay-attempt-evidence" ] || continue
    _marker="$_sd/.workbay-lane-sandbox"
    [ -f "$_marker" ] || continue
    _key="${_sd##*/}"
    # Skip non-allowlisted keys before lock/lease/proc-scan so a 90 s budget
    # can still reach later eligible sandboxes [COST-06].
    if [ "$dry_run" -eq 0 ] || [ "$authorized_only" -eq 1 ]; then
        case ",$eligible_keys," in
            *",$_key,"*) ;;
            *)
                skipped_not_allowlisted=$((skipped_not_allowlisted + 1))
                continue
                ;;
        esac
    fi
    candidates=$((candidates + 1))
    _glob_keys="${_glob_keys} ${_key}"
    if _lane_is_merged_marked "$_key"; then
        _merged_keys="${_merged_keys} ${_key}"
    else
        _mtime="$(_stat_mtime "$_marker" 2>/dev/null || echo 0)"
        case "$_mtime" in ''|*[!0-9]*) _mtime=0 ;; esac
        _unmerged_lines="${_unmerged_lines}${_mtime} ${_key}"$'\n'
    fi
done
_order_keys="$_glob_keys"
if [ "$pressured" -eq 1 ]; then
    _unmerged_sorted=$(printf '%s' "$_unmerged_lines" | LC_ALL=C sort -n | awk '{print $2}')
    _order_keys="${_merged_keys} ${_unmerged_sorted}"
fi
for _key in $_order_keys; do
    if [ "$(date +%s)" -ge "$reap_deadline" ]; then
        echo "remote_agent: reap budget exhausted; remaining sandboxes deferred" >&2
        _reap_probe_budget_deferred=1
        break
    fi
    _sd="$ROOT/$_key"
    _marker="$_sd/.workbay-lane-sandbox"
    [ -f "$_marker" ] || continue
    _lock="$ROOT/.lane-lock-$_key"
    lane_lock_fd=8
    exec 8>>"$_lock"
    if ! flock -n "$lane_lock_fd"; then
        skipped_locked=$((skipped_locked + 1))
        _reap_record_hold "$_key" held_lock
        exec 8>&-
        continue
    fi
    # Strong liveness signals outrank every clock. Malformed leases fail safe.
    # A live lease always wins over a merged marker. Exclusive flock above is
    # the lock-held proof for this authorized path (the dispatch body probes
    # `_lane_lock_held` without taking the lock).
    if _lane_occupant_live "$_key"; then
        skipped_live=$((skipped_live + 1))
        _reap_record_hold "$_key" held_live_lease
        flock -u "$lane_lock_fd" || true
        exec 8>&-
        continue
    fi
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
        skipped_live=$((skipped_live + 1))
        _probe_reason="proc_probe_unknown:${_PROC_CWD_PROBE_REASON:-proc_probe_failed}"
        _reap_record_hold "$_key" held_proc_probe_unknown "" "" "" 0 "$_probe_reason"
        if [ "${_reap_hold_journal_ok:-0}" -eq 1 ]; then
            _reap_probe_unknown_rows=$((_reap_probe_unknown_rows + 1))
        fi
        flock -u "$lane_lock_fd" || true
        exec 8>&-
        continue
    fi
    _mtime="$(_stat_mtime "$_marker" 2>/dev/null || true)"
    case "$_mtime" in
        ''|*[!0-9]*)
            skipped_live=$((skipped_live + 1))
            flock -u "$lane_lock_fd" || true
            exec 8>&-
            continue
            ;;
    esac
    _age=$((now - _mtime))
    _merged=0
    if _lane_is_merged_marked "$_key"; then
        _merged=1
    fi
    if [ "$_merged" -eq 0 ]; then
        if _pid_in_sandbox "$_sd"; then
            skipped_live=$((skipped_live + 1))
            if [ "${_PROC_CWD_PROBE_REASON:-}" = live_pid ]; then
                _reap_record_hold "$_key" held_live_process "$_PROC_CWD_MATCHED_PID" "$_PROC_CWD_MATCHED_COMM" "$_PROC_CWD_MATCHED_UID" "$_age"
            else
                _probe_reason="proc_probe_unknown:${_PROC_CWD_PROBE_REASON:-proc_probe_failed}"
                _reap_record_hold "$_key" held_proc_probe_unknown "" "" "" "$_age" "$_probe_reason"
                if [ "${_reap_hold_journal_ok:-0}" -eq 1 ]; then
                    _reap_probe_unknown_rows=$((_reap_probe_unknown_rows + 1))
                fi
            fi
            flock -u "$lane_lock_fd" || true
            exec 8>&-
            continue
        fi
        if [ "$_age" -le "$idle_limit" ]; then
            flock -u "$lane_lock_fd" || true
            exec 8>&-
            continue
        fi
    fi
    if [ "$dry_run" -eq 0 ] || [ "$authorized_only" -eq 1 ]; then
        case ",$eligible_keys," in
            *",$_key,"*) ;;
            *)
                flock -u "$lane_lock_fd" || true
                exec 8>&-
                continue
                ;;
        esac
    fi
    _venv="$ROOT/.venv-lane-$_key"
    _stamp="$ROOT/.venv-sync-stamp-$_key"
    _before="$(_du_bytes "$_sd" "$_venv" "$_stamp" 2>/dev/null || true)"
    case "$_before" in ''|*[!0-9]*) _before=0 ;; esac
    would_reap=$((would_reap + 1))
    would_free_bytes=$((would_free_bytes + _before))
    # ur-rp-30: a dry run lists its would-reap set here. An apply lists a sandbox only once
    # its last re-check has passed, just before the delete below, so a kept sandbox never
    # appears in candidates. would_reap and would_free_bytes keep counting every candidate.
    if [ "$dry_run" -eq 1 ]; then
        _reap_add_candidate "$_key" "$_before"
    fi
    if [ "$dry_run" -eq 0 ]; then
        _reason=sandbox_idle
        [ "$_merged" -eq 1 ] && _reason=sandbox_merged
        if [ "$_merged" -eq 1 ]; then
            if ! _lane_stop_processes "$_key" "$_sd"; then
                skipped_live=$((skipped_live + 1))
                _stop_outcome=kept_process_survived
                if [ "${_LANE_STOP_REASON:-}" = stop_budget_exhausted ]; then
                    _stop_outcome=kept_process_cleanup_budget_exhausted
                    _reason=stop_budget_exhausted
                fi
                printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_stop_outcome" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: kept_process_survived journal failed for $_key" >&2
                flock -u "$lane_lock_fd" || true
                exec 8>&-
                continue
            fi
            # CON-11: a pid can enter the sandbox after the pre-loop snapshot
            # (for example during the systemctl stop calls). Re-probe live
            # immediately before unlink. Distinct from kept_process_survived
            # (a snapshot pid that outlived TERM/KILL).
            if _pid_in_sandbox_live "$_sd"; then
                skipped_live=$((skipped_live + 1))
                _probe_outcome=kept_late_pid
                _probe_reason="$_reason"
                if [ -n "${_PROC_CWD_PROBE_REASON:-}" ] && [ "$_PROC_CWD_PROBE_REASON" != live_pid ]; then
                    _probe_outcome=kept_proc_probe_unknown
                    _probe_reason="proc_probe_unknown:${_PROC_CWD_PROBE_REASON}"
                fi
                printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_probe_outcome" "$_probe_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: $_probe_outcome journal failed for $_key" >&2
                flock -u "$lane_lock_fd" || true
                exec 8>&-
                continue
            fi
        else
            # CON-11: re-probe the TTL candidate immediately before unlink.
            # Do not terminate on the TTL-only path.
            if _pid_in_sandbox_live "$_sd"; then
                skipped_live=$((skipped_live + 1))
                _probe_outcome=kept_live_pid
                _probe_reason="$_reason"
                if [ -n "${_PROC_CWD_PROBE_REASON:-}" ] && [ "$_PROC_CWD_PROBE_REASON" != live_pid ]; then
                    _probe_outcome=kept_proc_probe_unknown
                    _probe_reason="proc_probe_unknown:${_PROC_CWD_PROBE_REASON}"
                fi
                printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_probe_outcome" "$_probe_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: $_probe_outcome journal failed for $_key" >&2
                flock -u "$lane_lock_fd" || true
                exec 8>&-
                continue
            fi
        fi
        if ! printf '{"key":"%s","outcome":"started","reason":"%s","age":%s,"actor":"maintenance","bytes_before":%s,"at":%s}\n' \
            "$_key" "$_reason" "$_age" "$_before" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            echo "remote_agent: cannot persist reap intent for $_key; deletion refused" >&2
            flock -u "$lane_lock_fd" || true
            exec 8>&-
            continue
        fi
        _reap_add_candidate "$_key" "$_before"
        rm -rf "$_sd" "$_venv" "$_stamp" 2>/dev/null || true
        if [ ! -e "$_sd" ]; then
            reaped=$((reaped + 1))
            bytes_freed=$((bytes_freed + _before))
            printf '{"key":"%s","outcome":"reaped","reason":"%s","age":%s,"actor":"maintenance","bytes_freed":%s,"at":%s}\n' \
                "$_key" "$_reason" "$_age" "$_before" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: reap completion journal failed for $_key" >&2
        else
            printf '{"key":"%s","outcome":"incomplete","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                "$_key" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: incomplete reap journal failed for $_key" >&2
        fi
    fi
    flock -u "$lane_lock_fd" || true
    exec 8>&-
done
if [ -n "$_reap_probe_unknown_reason" ] && \
    { [ "$_reap_probe_unknown_rows" -eq 0 ] || [ "$_reap_probe_budget_deferred" -eq 1 ]; }; then
    printf '{"outcome":"sweep_deferred","reason":"%s","actor":"maintenance","at":%s}\n' \
        "$_reap_probe_unknown_reason" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
        echo "remote_agent: sweep_deferred journal failed for $_reap_probe_unknown_reason" >&2
fi
orphans_killed=0
if [ "$dry_run" -eq 0 ]; then
    orphans_killed="$(_lane_reap_deleted_cwd_orphans)"
    case "$orphans_killed" in ''|*[!0-9]*) orphans_killed=0 ;; esac
fi
if [ "$dry_run" -eq 0 ]; then
    _maintenance_stop_overage_scopes
    _maintenance_reap_evidence
    _maintenance_gc_lease_locks
    _maintenance_reset_failed_units
fi
_freed_kb=$((bytes_freed / 1024))
if [ "$pressured" -eq 1 ]; then
    _pressured_json=true
else
    _pressured_json=false
fi
if ! printf '{"at":%s,"pressured":%s,"candidates":%s,"reaped":%s,"freed_kb":%s,"orphans_killed":%s}\n' \
    "$(date +%s)" "$_pressured_json" "$candidates" "$reaped" "$_freed_kb" "$orphans_killed" \
    >"$ROOT/.reap-last.json.tmp" || ! mv -f "$ROOT/.reap-last.json.tmp" "$ROOT/.reap-last.json"; then
    echo "remote_agent: cannot persist .reap-last.json" >&2
    rm -f "$ROOT/.reap-last.json.tmp" 2>/dev/null || true
    exit 75
fi

# RVUVC5-H-02: shared scratch roots are not owned by this pass. Only a
# per-run root with an explicit marker/name, an owner proof, and a writer-shared
# lock may be reclaimed. The newest descendant is the idle clock: a fresh output
# file keeps an old top-level pytest/run root alive. Every liveness decision is
# repeated while the writer lock is held immediately before the destructive arm.
uv_tmp_root="$tmp_root/workbay-uv-cache"

_scratch_pid_live() {
    _scratch_pid="$1"
    case "$_scratch_pid" in
        ''|0|*[!0-9]*) return 1 ;;
    esac
    kill -0 "$_scratch_pid" 2>/dev/null
}

_scratch_newest_mtime() {
    _scratch_dir="$1"
    _scratch_files="$(mktemp "${TMPDIR:-/tmp}/workbay-mtime.XXXXXX")" || return 1
    if ! find -P "$_scratch_dir" \
        ! -name '.workbay-scratch' \
        ! -name 'owner.pid' \
        ! -name '.owner.pid' \
        ! -name '.workbay-owner.pid' \
        -print0 >"$_scratch_files" 2>/dev/null; then
        rm -f "$_scratch_files"
        return 1
    fi
    _scratch_mtimes=
    _scratch_mtime_failed=0
    while IFS= read -r -d '' _scratch_entry; do
        _scratch_base="${_scratch_entry##*/}"
        case "$_scratch_base" in
            .workbay-scratch|owner.pid|.owner.pid|.workbay-owner.pid) continue ;;
        esac
        _scratch_entry_mtime="$(_stat_mtime "$_scratch_entry" 2>/dev/null || true)"
        case "$_scratch_entry_mtime" in
            ''|*[!0-9]*) _scratch_mtime_failed=1 ;;
            *) _scratch_mtimes="${_scratch_mtimes}${_scratch_entry_mtime}"$'\n' ;;
        esac
    done <"$_scratch_files"
    rm -f "$_scratch_files"
    [ "$_scratch_mtime_failed" -eq 0 ] || return 1
    printf '%s\n' "$_scratch_mtimes" | awk '
        BEGIN { max = -1; count = 0; invalid = 0 }
        NF == 0 { next }
        NF != 1 || $1 !~ /^[0-9]+$/ { invalid = 1; next }
        { count++; if ($1 > max) max = $1 }
        END { if (invalid || count == 0) exit 1; print max }
    '
}

# VMREAP-TMP-SCRATCH-UNSWEPT-01 item 4: this process's uid, resolved once.
# Empty means "unknowable"; the ownership probe then fails OPEN back to the
# pre-existing gates. That is safe because the probe only ever REFUSES
# deletion — it never grants any — so losing it cannot widen authority.
_reap_uid="$(id -u 2>/dev/null || true)"
case "$_reap_uid" in
    ''|*[!0-9]*)
        _reap_uid=
        echo "remote_agent: tmp_scratch_owner_probe_unavailable: id -u unusable" >&2
        ;;
esac
# Per-path foreign-owner reporting is bounded so one pass over a /tmp full of
# peer trees cannot flood the journal. The CAP APPLIES TO THE REPORT, NEVER TO
# THE COUNT: a receipt that stopped counting at the cap would understate what
# it saw, which is the exact defect this lane exists to remove.
_foreign_owner_report_cap=20
# Unnamed candidates are reported to STDERR only, never to
# `.reap-outcomes.jsonl`. Deletion-reconciliation evidence is the current file
# plus `.reap-outcomes.jsonl.1`, and on
# a box with ~90 unrecognized /tmp entries a per-pass journal row for each one
# would add ~1k rows/day of pure noise to the record that has to stay readable
# after a bad sweep. The durable, machine-readable fact is the COUNT, which
# rides in the receipt; the paths are an operator-facing diagnostic.
_unnamed_report_cap=20

# Classify one candidate's terminal disposition. Callers MUST pair this with
# `continue`; it cannot break the loop itself. An unknown bucket is loud rather
# than silently dropped, because a typo here would recreate the original defect
# in a subtler form: a candidate that is examined but accounted nowhere.
# $2 is the candidate path. The five buckets that carry bytes record it here, so
# the accounting phase can measure them later; no call site has to remember to.
# `unmeasurable` never gets bytes (its measurement already failed): it ignores the path.
_scratch_skip() {
    case "$1" in
        unnamed) tmp_scratch_skipped_unnamed=$((tmp_scratch_skipped_unnamed + 1)); _tmp_skip_unnamed+=("${2-}") ;;
        no_owner) tmp_scratch_skipped_no_owner=$((tmp_scratch_skipped_no_owner + 1)); _tmp_skip_no_owner+=("${2-}") ;;
        live) tmp_scratch_skipped_live=$((tmp_scratch_skipped_live + 1)); _tmp_skip_live+=("${2-}") ;;
        fresh) tmp_scratch_skipped_fresh=$((tmp_scratch_skipped_fresh + 1)); _tmp_skip_fresh+=("${2-}") ;;
        unmeasurable) tmp_scratch_skipped_unmeasurable=$((tmp_scratch_skipped_unmeasurable + 1)) ;;
        foreign_owner) tmp_scratch_skipped_foreign_owner=$((tmp_scratch_skipped_foreign_owner + 1)); _tmp_skip_foreign_owner+=("${2-}") ;;
        *) echo "remote_agent: scratch_skip_bucket_unknown: $1" >&2 ;;
    esac
    return 0
}

# ur-rp-23 accounting, run AFTER the reclaim loop (COST-06): it never runs inside
# the loop and never shares reap_deadline. Each collected path ends in exactly one
# of three states:
#   measured   du exited 0 and printed a number;
#   partial    du exited non-zero but still printed a number (EACCES inside a tree
#              we cannot read in full), so the bytes are a LOWER BOUND (CARD-06);
#   unmeasured timeout, no number, or the accounting deadline passed first.
# _du_bytes is not reused: it discards du's exit status, which is what separates
# measured from partial. Measures one path into one bucket.
_tmp_account_path() {
    local _tap_bucket="$1" _tap_path="$2" _tap_now _tap_left _tap_out _tap_rc=0 _tap_bytes
    _tap_now=$(date +%s)
    if [ "$_tap_now" -ge "$_tmp_acct_deadline" ]; then
        tmp_skipped_bytes_unmeasured=$((tmp_skipped_bytes_unmeasured + 1))
        tmp_skipped_bytes_truncated=1
        return 0
    fi
    # Both are whole epoch seconds and the guard above returned on now >= deadline, so this is >= 1.
    _tap_left=$((_tmp_acct_deadline - _tap_now))
    _tap_out="$(timeout -k 1 "$_tap_left" du -sb -- "$_tap_path" 2>/dev/null)" || _tap_rc=$?
    case "$_tap_rc" in
        124|137)
            # The timeout is the accounting window cutting this measurement short (CARD-06).
            tmp_skipped_bytes_unmeasured=$((tmp_skipped_bytes_unmeasured + 1))
            tmp_skipped_bytes_truncated=1
            return 0
            ;;
    esac
    _tap_bytes="${_tap_out%%[[:space:]]*}"
    case "$_tap_bytes" in ''|*[!0-9]*)
        tmp_skipped_bytes_unmeasured=$((tmp_skipped_bytes_unmeasured + 1))
        return 0
        ;;
    esac
    case "$_tap_bucket" in
        unnamed) tmp_scratch_skipped_unnamed_bytes=$((tmp_scratch_skipped_unnamed_bytes + _tap_bytes)) ;;
        no_owner) tmp_scratch_skipped_no_owner_bytes=$((tmp_scratch_skipped_no_owner_bytes + _tap_bytes)) ;;
        fresh) tmp_scratch_skipped_fresh_bytes=$((tmp_scratch_skipped_fresh_bytes + _tap_bytes)) ;;
        live) tmp_scratch_skipped_live_bytes=$((tmp_scratch_skipped_live_bytes + _tap_bytes)) ;;
        foreign_owner) tmp_scratch_skipped_foreign_owner_bytes=$((tmp_scratch_skipped_foreign_owner_bytes + _tap_bytes)) ;;
    esac
    if [ "$_tap_rc" -ne 0 ]; then
        tmp_skipped_bytes_partial=$((tmp_skipped_bytes_partial + 1))
    fi
    return 0
}

# Headline buckets first (unnamed, no_owner), then the rest. The budget is
# WORKBAY_REMOTE_AGENT_TMP_ACCOUNTING_SEC (default 20; non-numeric falls back to the
# default; 0 disables). Disabled, or without timeout(1), every collected path is
# counted unmeasured and the receipt says truncated: nothing is guessed. A caller that
# stated its wait (_reap_caller_budget) also bounds the window by that wait, less a
# headroom; a window already spent measures nothing and says so. reap_deadline is untouched.
# A reclaim scan that stopped early (tmp_scratch_scan_truncated) collected only a prefix of
# $tmp_root, so its bytes are a lower bound too and the receipt says truncated.
_tmp_account_skipped() {
    local _tas_sec="${WORKBAY_REMOTE_AGENT_TMP_ACCOUNTING_SEC:-20}" _tas_path _tas_now _tas_left _tas_window
    # SSH setup runs before this body starts; the receipt transfer runs after it. Neither is in the window.
    local _tas_headroom=10
    case "$_tas_sec" in *[!0-9]*|"") _tas_sec=20 ;; esac
    _tas_sec=$((10#$_tas_sec))
    _tmp_acct_deadline=0
    [ "$tmp_scratch_scan_truncated" -eq 0 ] || tmp_skipped_bytes_truncated=1
    if [ "$_tas_sec" -eq 0 ]; then
        tmp_skipped_bytes_truncated=1
    elif ! command -v timeout >/dev/null 2>&1; then
        echo "remote_agent: tmp_accounting_unavailable: timeout missing" >&2
        tmp_skipped_bytes_truncated=1
    else
        _tas_now=$(date +%s)
        _tas_window=$_tas_sec
        if [ -n "$_reap_caller_budget" ]; then
            _tas_left=$((_reap_body_start + _reap_caller_budget - _tas_headroom - _tas_now))
            [ "$_tas_left" -ge "$_tas_window" ] || _tas_window=$_tas_left
        fi
        if [ "$_tas_window" -gt 0 ]; then
            _tmp_acct_deadline=$((_tas_now + _tas_window))
        else
            echo "remote_agent: tmp_accounting_skipped: caller budget exhausted" >&2
            tmp_skipped_bytes_truncated=1
        fi
    fi
    for _tas_path in ${_tmp_skip_unnamed[@]+"${_tmp_skip_unnamed[@]}"}; do
        _tmp_account_path unnamed "$_tas_path"
    done
    for _tas_path in ${_tmp_skip_no_owner[@]+"${_tmp_skip_no_owner[@]}"}; do
        _tmp_account_path no_owner "$_tas_path"
    done
    for _tas_path in ${_tmp_skip_fresh[@]+"${_tmp_skip_fresh[@]}"}; do
        _tmp_account_path fresh "$_tas_path"
    done
    for _tas_path in ${_tmp_skip_live[@]+"${_tmp_skip_live[@]}"}; do
        _tmp_account_path live "$_tas_path"
    done
    for _tas_path in ${_tmp_skip_foreign_owner[@]+"${_tmp_skip_foreign_owner[@]}"}; do
        _tmp_account_path foreign_owner "$_tas_path"
    done
    tmp_unowned_bytes=$((tmp_scratch_skipped_unnamed_bytes + tmp_scratch_skipped_no_owner_bytes))
}

for _scratch_path in "$tmp_root"/*/; do
    if [ "$(date +%s)" -ge "$reap_deadline" ]; then
        tmp_scratch_scan_truncated=1
        break
    fi
    [ -d "$_scratch_path" ] || continue
    _scratch="${_scratch_path%/}"
    [ "$_scratch" = "$uv_tmp_root" ] && continue
    [ ! -L "$_scratch" ] || continue
    _tmp_scratch_scan_seen=$((_tmp_scratch_scan_seen + 1))
    if [ "$dry_run" -eq 1 ] && [ "$_tmp_scratch_scan_seen" -gt 50 ]; then
        tmp_scratch_scan_truncated=1
        echo 'remote_agent: tmp_scratch_scan_truncated: dry-run candidate scan capped at 50' >&2
        break
    fi
    # From here on the entry is a real, non-symlink directory under $tmp_root
    # that is not the uv cache: it has been EXAMINED. Every path out of the
    # loop below must therefore land in exactly one bucket.
    tmp_scratch_candidates_seen=$((tmp_scratch_candidates_seen + 1))
    # Ownership is probed BEFORE the naming gate on purpose. On 2026-09-07
    # ~3.9G of /tmp was `ubuntu`-owned while this runs as `gate`; those trees
    # are unreclaimable under ANY naming rule, and filing them as "unrecognized
    # name" would hide the one fact an operator can act on. Reclaiming them is
    # an operator decision: there is deliberately no privilege escalation here.
    if [ -n "$_reap_uid" ]; then
        _scratch_uid="$(_stat_uid "$_scratch" 2>/dev/null || true)"
        case "$_scratch_uid" in
            ''|*[!0-9]*)
                _scratch_skip unmeasurable "$_scratch"
                continue
                ;;
        esac
        if [ "$_scratch_uid" != "$_reap_uid" ]; then
            _scratch_skip foreign_owner "$_scratch"
            if [ "$tmp_scratch_skipped_foreign_owner" -le "$_foreign_owner_report_cap" ]; then
                echo "remote_agent: tmp_scratch_foreign_owner: uid=$_scratch_uid path=$_scratch" >&2
                printf '{"path":"%s","outcome":"skipped_foreign_owner","uid":%s,"at":%s}\n' \
                    "$_scratch" "$_scratch_uid" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: foreign-owner journal failed for $_scratch" >&2
            elif [ "$tmp_scratch_skipped_foreign_owner" -eq $((_foreign_owner_report_cap + 1)) ]; then
                echo "remote_agent: tmp_scratch_foreign_owner_truncated: per-path reporting capped at $_foreign_owner_report_cap; see tmp_scratch_skipped_foreign_owner for the full count" >&2
            fi
            continue
        fi
    fi
    _scratch_name="${_scratch##*/}"
    _scratch_marker="$_scratch/.workbay-scratch"
    _scratch_known=0
    case "$_scratch_name" in
        workbay-run-*|workbay-scratch-*) _scratch_known=1 ;;
    esac
    if [ "$_scratch_known" -ne 1 ] && [ ! -f "$_scratch_marker" ]; then
        _scratch_skip unnamed "$_scratch"
        # `skipped_unnamed: 90` still leaves an operator hunting; the PATHS are
        # what closed the 2026-09-07 incident by hand. Naming a tree grants no
        # authority over it, so this is free to be as loud as the cap allows.
        # The gate itself is deliberately NOT widened to match these names:
        # none of them has a producer in this repository (the tree contains no
        # `mktemp -d` in executable code at all), so any glob covering them
        # would be deletion authority granted on a guess — and would rot the
        # moment the next wave picks new ids. A producer that wants its tree
        # reclaimed stamps `.workbay-scratch`; that path is name-agnostic and
        # opt-in by construction.
        if [ "$tmp_scratch_skipped_unnamed" -le "$_unnamed_report_cap" ]; then
            echo "remote_agent: tmp_scratch_unnamed: no producer marker or known name; path=$_scratch" >&2
        elif [ "$tmp_scratch_skipped_unnamed" -eq $((_unnamed_report_cap + 1)) ]; then
            echo "remote_agent: tmp_scratch_unnamed_truncated: per-path reporting capped at $_unnamed_report_cap; see tmp_scratch_skipped_unnamed for the full count" >&2
        fi
        continue
    fi

    _scratch_key=
    _scratch_pid=
    if [ -f "$_scratch_marker" ]; then
        while IFS= read -r _scratch_line || [ -n "$_scratch_line" ]; do
            case "$_scratch_line" in
                lane_key=*) _scratch_key="${_scratch_line#lane_key=}" ;;
                pid=*) _scratch_pid="${_scratch_line#pid=}" ;;
            esac
        done <"$_scratch_marker" || { _scratch_skip no_owner "$_scratch"; continue; }
    fi
    case "$_scratch_key" in
        "")
            case "$_scratch_name" in
                workbay-run-*) _scratch_key="${_scratch_name#workbay-run-}" ;;
                workbay-scratch-*) _scratch_key="${_scratch_name#workbay-scratch-}" ;;
            esac
            ;;
    esac
    for _owner_candidate in \
        "$_scratch/owner.pid" \
        "$_scratch/.owner.pid" \
        "$_scratch/.workbay-owner.pid"; do
        [ -f "$_owner_candidate" ] || continue
        _owner_value="$(head -n 1 "$_owner_candidate" 2>/dev/null || true)"
        [ -n "$_scratch_pid" ] || _scratch_pid="$_owner_value"
        break
    done
    case "$_scratch_key" in
        ""|*[!A-Za-z0-9._-]*) _scratch_skip no_owner "$_scratch"; continue ;;
    esac
    _scratch_has_owner=0
    case "$_scratch_pid" in
        ''|0|*[!0-9]*)
            if [ ! -f "$ROOT/.lane-live-$_scratch_key" ]; then
                _scratch_skip no_owner "$_scratch"
                continue
            fi
            ;;
        *) _scratch_has_owner=1 ;;
    esac
    [ -f "$ROOT/.lane-live-$_scratch_key" ] && _scratch_has_owner=1
    if [ "$_scratch_has_owner" -ne 1 ]; then
        _scratch_skip no_owner "$_scratch"
        continue
    fi
    if _scratch_pid_live "$_scratch_pid" || _lane_occupant_live "$_scratch_key" || _pid_in_sandbox "$_scratch"; then
        _scratch_skip live "$_scratch"
        continue
    fi
    # A tree whose newest mtime cannot be derived is unmeasurable, not idle:
    # refuse it, but say so rather than folding it into the same bare zero.
    _scratch_mtime="$(_scratch_newest_mtime "$_scratch")" || {
        _scratch_skip unmeasurable "$_scratch"
        continue
    }
    case "$_scratch_mtime" in ''|*[!0-9]*) _scratch_skip unmeasurable "$_scratch"; continue ;; esac
    _scratch_age=$((now - _scratch_mtime))
    if [ "$_scratch_age" -le "$idle_limit" ]; then
        _scratch_skip fresh "$_scratch"
        continue
    fi

    _scratch_lock="$ROOT/.lane-lock-$_scratch_key"
    if [ -L "$_scratch_lock" ]; then
        _scratch_skip unmeasurable "$_scratch"
        continue
    fi
    scratch_lock_fd=8
    if ! exec 8>>"$_scratch_lock"; then
        _scratch_skip unmeasurable "$_scratch"
        continue
    fi
    # Lock contention is a live writer, not an absence of candidates.
    if ! flock -n "$scratch_lock_fd"; then
        exec 8>&-
        _scratch_skip live "$_scratch"
        continue
    fi
    scratch_local_lock_fd=6
    scratch_local_lock=
    if [ -e "$_scratch/.lock" ]; then
        # Only a regular file that is not a symlink is opened here. The agent owns
        # this tree and can replace .lock with a FIFO as its last act; opening a
        # FIFO for write blocks with no reader, which would hang the sweep while it
        # holds fds 7 and 8 [RES-03]. Anything else releases both locks and is
        # skipped as unmeasurable, the same as a symlink.
        [ -f "$_scratch/.lock" ] && [ ! -L "$_scratch/.lock" ] || {
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip unmeasurable "$_scratch"
            continue
        }
        scratch_local_lock="$_scratch/.lock"
        # O_RDWR (<>), not O_WRONLY (>>): bash has no O_NONBLOCK redirect, and Linux
        # (fifo(7)) opens a FIFO O_RDWR without blocking, so this open cannot hang
        # even if .lock was swapped after the test above. The held fd is then
        # checked as a regular file. /dev/fd/6 stats the inode behind the fd, not
        # the path, so that check cannot be raced: a swap is skipped, never held.
        if ! exec 6<>"$scratch_local_lock"; then
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip live "$_scratch"
            continue
        fi
        if [ ! -f /dev/fd/6 ]; then
            exec 6>&-
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip unmeasurable "$_scratch"
            continue
        fi
        if ! flock -n "$scratch_local_lock_fd"; then
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            exec 6>&-
            _scratch_skip live "$_scratch"
            continue
        fi
    fi
    # Re-probe all ownership/liveness signals under the writer lock [CON-11].
    if _scratch_pid_live "$_scratch_pid" || _lane_occupant_live "$_scratch_key" || _pid_in_sandbox "$_scratch"; then
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip live "$_scratch"
        continue
    fi
    _scratch_mtime="$(_scratch_newest_mtime "$_scratch")" || _scratch_mtime=
    case "$_scratch_mtime" in
        ''|*[!0-9]*)
            [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip unmeasurable "$_scratch"
            continue
            ;;
    esac
    _scratch_age=$((now - _scratch_mtime))
    if [ "$_scratch_age" -le "$idle_limit" ]; then
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip fresh "$_scratch"
        continue
    fi
    _scratch_before="$(_du_bytes "$_scratch" 2>/dev/null || true)"
    case "$_scratch_before" in ''|*[!0-9]*)
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip unmeasurable "$_scratch"
        continue
        ;;
    esac
    would_tmp_scratch_reaped=$((would_tmp_scratch_reaped + 1))
    would_tmp_scratch_bytes=$((would_tmp_scratch_bytes + _scratch_before))
    if [ "$dry_run" -eq 0 ]; then
        if ! printf '{"path":"%s","outcome":"started","bytes_before":%s,"at":%s}\n' \
            "$_scratch" "$_scratch_before" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
            echo "remote_agent: cannot persist scratch reap intent for $_scratch; deletion refused" >&2
        else
            rm -rf -- "$_scratch" 2>/dev/null || true
            if [ ! -e "$_scratch" ]; then
                tmp_scratch_reaped=$((tmp_scratch_reaped + 1))
                tmp_scratch_bytes=$((tmp_scratch_bytes + _scratch_before))
                printf '{"path":"%s","outcome":"reaped","bytes_freed":%s,"at":%s}\n' \
                    "$_scratch" "$_scratch_before" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            else
                printf '{"path":"%s","outcome":"incomplete","at":%s}\n' \
                    "$_scratch" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
            fi
        fi
    fi
    [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
    flock -u "$scratch_lock_fd" || true
    exec 8>&-
done

# Probe uv's cache-use lock before native pruning. Age cannot prove that
# installed environments no longer reference archives (RVUVC4-H-01).
# RVUVC5-H-03: validate the owned leaf against its canonical parent, open that
# directory once, and pass the stable directory fd to uv. A later replacement
# of the public cache name can therefore not redirect native deletion.
_reap_cwd="$PWD"
_uv_tmp_real="$(cd -P -- "$tmp_root" 2>/dev/null && pwd -P)" || _uv_tmp_real=
_uv_expected_root="$_uv_tmp_real/workbay-uv-cache"
if [ -n "$_uv_tmp_real" ] && [ ! -L "$uv_tmp_root" ] && [ -d "$uv_tmp_root" ] &&
    find -P "$uv_tmp_root" -maxdepth 0 -type d -exec test ! -L {} \; 2>/dev/null &&
    [ "$(cd -P -- "$uv_tmp_root" 2>/dev/null && pwd -P)" = "$_uv_expected_root" ]; then
    _uv_path_identity="$(stat -Lc '%d:%i' "$uv_tmp_root" 2>/dev/null)" || _uv_path_identity=
    if [ -n "$_uv_path_identity" ] && { exec 6<"$uv_tmp_root"; }; then
        _uv_fd_identity="$(stat -Lc '%d:%i' /proc/self/fd/6 2>/dev/null)" || _uv_fd_identity=
        if [ -n "$_uv_fd_identity" ] && [ "$_uv_path_identity" = "$_uv_fd_identity" ]; then
            _uv_lock_path="/proc/self/fd/6/.lock"
            if [ ! -L "$_uv_lock_path" ] &&
                { [ ! -e "$_uv_lock_path" ] || [ -f "$_uv_lock_path" ]; } &&
                { exec 5>>"$_uv_lock_path"; } && flock -n 5; then
                # Native uv takes this same lock itself. Release the admission
                # probe before invoking it; timeout bounds later contention.
                flock -u 5
                exec 5>&-
                _uv_remaining=$((reap_deadline - $(date +%s) - 5))
                if [ "$dry_run" -eq 1 ]; then
                    # uv has no prune dry-run contract. Unknown savings remain zero.
                    echo "remote_agent: uv_cache_dry_run: native prune deferred" >&2
                elif [ "$_uv_remaining" -le 0 ]; then
                    echo "remote_agent: uv_cache_budget_exhausted: native prune deferred" >&2
                elif ! command -v uv >/dev/null 2>&1 || ! command -v timeout >/dev/null 2>&1; then
                    echo "remote_agent: uv_cache_prune_unavailable: uv and timeout required; preserving cache" >&2
                elif printf '{"outcome":"uv_cache_started","at":%s}\n' "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl"; then
                    # Re-stat immediately before the native destructive operation.
                    _uv_before_delete_identity="$(stat -Lc '%d:%i' /proc/self/fd/6 2>/dev/null)" || _uv_before_delete_identity=
                    if [ "$_uv_before_delete_identity" != "$_uv_fd_identity" ]; then
                        echo "remote_agent: uv_cache_root_changed: preserving cache" >&2
                    elif (cd -- "$ROOT" && UV_CACHE_DIR=/proc/self/fd/6 timeout -k 5 "$_uv_remaining" uv cache prune >&2 </dev/null 5>&- 7>&-); then
                        # uv's human-readable size output is version-dependent;
                        # report unknown savings as zero rather than guessing.
                        printf '{"outcome":"uv_cache_reaped","bytes_freed":0,"at":%s}\n' \
                            "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || true
                    else
                        _uv_rc=$?
                        echo "remote_agent: uv_cache_prune_failed: status=$_uv_rc" >&2
                    fi
                else
                    echo "remote_agent: cannot persist uv cache reap intent; pruning refused" >&2
                fi
            else
                echo "remote_agent: uv_cache_in_use: cache lock unavailable; preserving cache" >&2
            fi
        else
            echo "remote_agent: uv_cache_root_changed: preserving cache" >&2
        fi
        exec 6>&-
    fi
fi
if [ "$dry_run" -eq 0 ]; then _prune_probe_trust || true; fi
cd -- "$_reap_cwd"
sandbox_count_after=$(_sandbox_count)
# ur-rp-23: every reclaim action above has finished. Accounting only reads (du -sb), so
# drop the reap mutex first: holding it would defer the dispatch-side LRU reclaim and
# turn concurrent reaps away (exit 75) for the whole window, when /tmp is already full.
# A tree another reaper removes meanwhile simply ends up unmeasured.
flock -u "$reap_lock_fd" || true
exec 7>&-
_tmp_account_skipped
printf '{"reaped":%s,"would_reap":%s,"skipped_live":%s,"skipped_locked":%s,"bytes_freed":%s,"would_free_bytes":%s,"sandbox_count_after":%s,"uv_cache_bytes":%s,"tmp_scratch_bytes":%s,"tmp_scratch_reaped":%s,"would_uv_cache_bytes":%s,"would_tmp_scratch_bytes":%s,"would_tmp_scratch_reaped":%s,"tmp_scratch_candidates_seen":%s,"tmp_scratch_scan_truncated":%s,"tmp_scratch_skipped_unnamed":%s,"tmp_scratch_skipped_no_owner":%s,"tmp_scratch_skipped_live":%s,"tmp_scratch_skipped_fresh":%s,"tmp_scratch_skipped_unmeasurable":%s,"tmp_scratch_skipped_foreign_owner":%s,"tmp_scratch_skipped_unnamed_bytes":%s,"tmp_scratch_skipped_no_owner_bytes":%s,"tmp_scratch_skipped_fresh_bytes":%s,"tmp_scratch_skipped_live_bytes":%s,"tmp_scratch_skipped_foreign_owner_bytes":%s,"tmp_unowned_bytes":%s,"tmp_skipped_bytes_unmeasured":%s,"tmp_skipped_bytes_partial":%s,"tmp_skipped_bytes_truncated":%s,"liveness_examined":%s,"liveness_unreadable":%s,"skipped_not_allowlisted":%s,"holds":[%s],"holds_truncated":%s,"candidates":[%s],"candidates_truncated":%s,"mode":"%s"%s%s%s}\n' \
    "$reaped" "$would_reap" "$skipped_live" "$skipped_locked" "$bytes_freed" "$would_free_bytes" "$sandbox_count_after" \
    "$uv_cache_bytes" "$tmp_scratch_bytes" "$tmp_scratch_reaped" "$would_uv_cache_bytes" "$would_tmp_scratch_bytes" "$would_tmp_scratch_reaped" \
    "$tmp_scratch_candidates_seen" "$tmp_scratch_scan_truncated" "$tmp_scratch_skipped_unnamed" "$tmp_scratch_skipped_no_owner" \
    "$tmp_scratch_skipped_live" "$tmp_scratch_skipped_fresh" "$tmp_scratch_skipped_unmeasurable" \
    "$tmp_scratch_skipped_foreign_owner" "$tmp_scratch_skipped_unnamed_bytes" "$tmp_scratch_skipped_no_owner_bytes" \
    "$tmp_scratch_skipped_fresh_bytes" "$tmp_scratch_skipped_live_bytes" "$tmp_scratch_skipped_foreign_owner_bytes" \
    "$tmp_unowned_bytes" "$tmp_skipped_bytes_unmeasured" "$tmp_skipped_bytes_partial" "$tmp_skipped_bytes_truncated" \
    "$_reap_liveness_examined" "$_reap_liveness_unreadable" \
    "$skipped_not_allowlisted" "$_reap_holds_json" "$_reap_holds_truncated" \
    "$_reap_candidates_json" "$_reap_candidates_truncated" "$reap_mode" "$_reap_degraded_field" "$_reap_script_sha_field" \
    "$_reap_liveness_coverage_field"
REAP_EOF
    } | "${SSH[@]}" bash -s -- "$AGENT_ROOT" "$REAP_IDLE" "$REAP_DRY" \
        "$REAP_AUTHORIZED_ONLY::/tmp:${_reap_script_sha256}:${WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S}:${_reap_outcomes_cap}" \
        "$REAP_CALLER_BUDGET"
    ;;
status)
    exec </dev/null
    STATUS_SWEEP=0
    STATUS_LIST=0
    STATUS_JOB_IDS=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --job-id)
                _jid="${2:-}"
                if ! [[ "$_jid" =~ ^j[0-9a-f]{20}$ ]]; then
                    die "invalid --job-id (need j + 20 lowercase hex)"
                fi
                STATUS_JOB_IDS="${STATUS_JOB_IDS:+$STATUS_JOB_IDS }${_jid}"
                shift 2
                ;;
            --sweep)
                STATUS_SWEEP=1
                shift
                ;;
            --list)
                STATUS_LIST=1
                shift
                ;;
            *) die "unknown arg: $1" ;;
        esac
    done
    # --list is the read-only enumeration: refuse the removing/selecting flags
    # before any ssh round trip (the VM op refuses them again).
    _status_bound_env=()
    if [ "$STATUS_LIST" = 1 ]; then
        [ "$STATUS_SWEEP" != 1 ] || die "list_sweep_refused: --list is read-only; run status --sweep separately"
        [ -z "$STATUS_JOB_IDS" ] || die "list_job_id_refused: --list enumerates every job; do not pass --job-id"
        # The VM shell does not inherit this environment, so the list bounds ride
        # the ssh env call, only when set. Junk is refused here, before any ssh
        # round trip; the VM still clamps a huge value to its documented ceiling.
        if [ -n "${STATUS_LIST_AFTER+x}" ]; then
            _status_cursor_re='^[01][012]-([0-9a-f]{2}){1,255}$'
            [ "$STATUS_LIST_AFTER" = start ] || [[ "$STATUS_LIST_AFTER" =~ $_status_cursor_re ]] || die "list_cursor_invalid: STATUS_LIST_AFTER must be start or a basename cursor"
            _status_bound_env+=("STATUS_LIST_AFTER=${STATUS_LIST_AFTER}")
        fi
        if [ -n "${STATUS_LIST_MAX+x}" ]; then
            [[ "$STATUS_LIST_MAX" =~ ^[1-9][0-9]*$ ]] || die "list_max_invalid: STATUS_LIST_MAX must be a positive integer (got $(printf %q "$STATUS_LIST_MAX"))"
            _status_bound_env+=("STATUS_LIST_MAX=${STATUS_LIST_MAX}")
        fi
        if [ -n "${STATUS_LIST_DEADLINE_S+x}" ]; then
            [[ "$STATUS_LIST_DEADLINE_S" =~ ^[1-9][0-9]*$ ]] || die "list_deadline_invalid: STATUS_LIST_DEADLINE_S must be a positive integer of seconds (got $(printf %q "$STATUS_LIST_DEADLINE_S"))"
            _status_bound_env+=("STATUS_LIST_DEADLINE_S=${STATUS_LIST_DEADLINE_S}")
        fi
    elif [ -z "$STATUS_JOB_IDS" ] && [ "$STATUS_SWEEP" != 1 ]; then
        die "--job-id, --sweep or --list required"
    fi
    _JOB_OPS_SRC="$(declare -f _lane_scope_cgroup_members _remote_job_ops)"
    _status_rc=0
    # SSH joins command argv for a remote shell. Keep values out of that
    # command: quote complete export assignments for the Bash stdin script.
    _status_exports="$(printf 'export %q\n' \
        "AGENT_ROOT=${AGENT_ROOT}" \
        "STATUS_JOB_IDS=${STATUS_JOB_IDS}" \
        "STATUS_SWEEP=${STATUS_SWEEP}" \
        "STATUS_LIST=${STATUS_LIST}" \
        "REMOTE_JOB_OP=status" \
        ${_status_bound_env[@]+"${_status_bound_env[@]}"})"
    "${SSH[@]}" bash -s <<STATUS_EOF || _status_rc=$?
${_status_exports}
set -euo pipefail
${_JOB_OPS_SRC}
_remote_job_ops
STATUS_EOF
    if [ "$_status_rc" -ne 0 ]; then
        echo "remote_agent: status_failed rc=${_status_rc}" >&2
        exit "$_status_rc"
    fi
    ;;
collect)
    exec </dev/null
    JOB_ID=""
    STAGE_DIR=""
    _destinations=()
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --job-id)
                JOB_ID="${2:-}"
                shift 2
                ;;
            --stage-dir)
                [ "$#" -ge 2 ] && [ -n "$2" ] || die "missing stage directory"
                STAGE_DIR="$2"; shift 2 ;;
            --out|--result-out|--debug-out|--stream-out|--selfverify-out|--uncommitted-out|--phases-out|--provenance-out)
                [ "$#" -ge 2 ] && [ -n "$2" ] || die "missing artifact destination"
                _destinations+=("$1" "$2"); shift 2 ;;
            *) die "unknown arg: $1" ;;
        esac
    done
    [ -n "$JOB_ID" ] || die "--job-id required"
    if ! [[ "$JOB_ID" =~ ^j[0-9a-f]{20}$ ]]; then
        die "invalid --job-id (need j + 20 lowercase hex)"
    fi
    if [ -n "$STAGE_DIR" ]; then
        # Explicitly governed pure staging (terminal-evidence materialization):
        # the same stream and bundle/done/stdout/manifest validation as collect,
        # then the validated bundle is written under STAGE_DIR only. It never
        # prints stdout, installs destinations, enters the review-cycle bridge
        # or stamps the VM job's collected_at (retained remote custody holds).
        [ -z "${_destinations[*]+x}" ] || die "--stage-dir refuses artifact destinations"
        [ -d "$STAGE_DIR" ] && [ ! -L "$STAGE_DIR" ] || die "--stage-dir must be an existing directory"
    fi
    _JOB_OPS_SRC="$(declare -f _lane_scope_cgroup_members _remote_job_ops)"
    _collect_tmp="$(mktemp -d "${TMPDIR:-/tmp}/wb-collect.XXXXXX")"
    _stream_rc=0
    # shellcheck disable=SC2029
    "${SSH[@]}" env \
        "AGENT_ROOT=${AGENT_ROOT}" \
        "JOB_ID=${JOB_ID}" \
        "COLLECT_MODE=stream" \
        "REMOTE_JOB_OP=collect" \
        bash -s <<COLLECT_EOF >"${_collect_tmp}/bundle.tar" 2>"${_collect_tmp}/remote.err" || _stream_rc=$?
set -euo pipefail
${_JOB_OPS_SRC}
_remote_job_ops
COLLECT_EOF
    if [ -s "${_collect_tmp}/remote.err" ]; then
        cat "${_collect_tmp}/remote.err" >&2
    fi
    if [ "$_stream_rc" -eq 75 ]; then
        echo "remote_agent: job_not_done" >&2
        rm -rf "$_collect_tmp"
        exit 75
    fi
    if [ "$_stream_rc" -ne 0 ]; then
        echo "remote_agent: collect_failed rc=${_stream_rc}" >&2
        rm -rf "$_collect_tmp"
        if [ "$_stream_rc" -eq 75 ]; then
            exit 75
        fi
        exit "$_stream_rc"
    fi
    mkdir -p "${_collect_tmp}/out"
    if ! python3 - "${_collect_tmp}/bundle.tar" "${_collect_tmp}/out" <<'PYEXTRACT'
import sys
import tarfile
from pathlib import Path
allowed = {"done.json", "stdout", "stderr", "artifacts.json"}
keys = {"result-out", "debug-out", "stream-out", "selfverify-out", "uncommitted-out", "phases-out", "provenance-out"}
allowed.update("artifacts/" + key for key in keys)
with tarfile.open(sys.argv[1]) as bundle:
    seen = set()
    for member in bundle:
        if member.name == "artifacts" and member.isdir():
            continue
        if member.name not in allowed or not member.isfile() or member.name in seen:
            raise ValueError("invalid collection member")
        seen.add(member.name)
        target = Path(sys.argv[2]) / member.name
        target.parent.mkdir(exist_ok=True)
        target.write_bytes(bundle.extractfile(member).read())
PYEXTRACT
    then
        echo "remote_agent: collect_bundle_unreadable" >&2
        rm -rf "$_collect_tmp"
        exit 5
    fi
    if [ ! -f "${_collect_tmp}/out/done.json" ]; then
        echo "remote_agent: job_not_done" >&2
        rm -rf "$_collect_tmp"
        exit 75
    fi
    [ -f "${_collect_tmp}/out/stdout" ] || : >"${_collect_tmp}/out/stdout"
    [ -f "${_collect_tmp}/out/stderr" ] || : >"${_collect_tmp}/out/stderr"
    _parse_rc=0
    _want="$(WORKBAY_JSON_FILE="${_collect_tmp}/out/done.json" WORKBAY_JOB_ID="$JOB_ID" python3 - <<'PY'
import json
import os
import re
import sys

def fail():
    print("remote_agent: invalid_done", file=sys.stderr)
    sys.exit(5)

path = os.environ.get("WORKBAY_JSON_FILE", "")
want_id = os.environ.get("WORKBAY_JOB_ID", "")
try:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
except Exception:
    fail()
if not isinstance(data, dict):
    fail()
job_id = data.get("job_id")
rc = data.get("rc")
sha = data.get("stdout_sha256")
if not isinstance(job_id, str) or job_id != want_id:
    fail()
if isinstance(rc, bool) or not isinstance(rc, int) or rc < 0 or rc > 255:
    fail()
if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None:
    fail()
print(sha)
print(rc)
PY
)" || _parse_rc=$?
    if [ "$_parse_rc" -ne 0 ]; then
        echo "remote_agent: invalid_done" >&2
        rm -rf "$_collect_tmp"
        exit 5
    fi
    # A read-only analysis is mediated again at collection (SECD-03): its
    # non-empty stdout (commits or an uncommitted tree diff) never reaches
    # stdout or --out. The spool keeps it as the charged attempt's settlement
    # evidence and collection exits 76.
    _collect_analysis_refused=0
    _collect_authority="$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)/workbay/review_cycle_dispatch"
    _collect_key="$(printf '%s' "$JOB_ID" | shasum -a 256 | cut -d ' ' -f 1)"
    if [ -n "$STAGE_DIR" ]; then
        :  # pure staging: stdout is staged below, never emitted or spooled
    elif [ -s "${_collect_tmp}/out/stdout" ] && python3 - "${_collect_authority}/${_collect_key}.json" <<'PY'
import json, sys

try:
    kind = json.load(open(sys.argv[1], encoding="utf-8"))["identity"]["kind"]
except (OSError, ValueError, KeyError, TypeError):
    kind = None
sys.exit(0 if kind == "analysis" else 1)
PY
    then
        _collect_analysis_refused=1
        mkdir -p "${_collect_authority}/spool-${_collect_key}"
        cp "${_collect_tmp}/out/stdout" "${_collect_authority}/spool-${_collect_key}/refused-analysis.patch"
        _kept_destinations=()
        set -- ${_destinations[@]+"${_destinations[@]}"}
        while [ "$#" -gt 0 ]; do
            if [ "$1" = --out ]; then rm -f "$2"; else _kept_destinations+=("$1" "$2"); fi
            shift 2
        done
        _destinations=(${_kept_destinations[@]+"${_kept_destinations[@]}"})
    else
        cat "${_collect_tmp}/out/stdout"
    fi
    cat "${_collect_tmp}/out/stderr" >&2
    _want_sha="$(printf '%s\n' "$_want" | sed -n '1p')"
    _recorded_rc="$(printf '%s\n' "$_want" | sed -n '2p')"
    _got="$(sha256sum "${_collect_tmp}/out/stdout" 2>/dev/null | awk '{print $1}')"
    if [ -z "$_want_sha" ] || [ "$_want_sha" != "$_got" ]; then
        echo "remote_agent: stdout_sha256_mismatch" >&2
        rm -rf "$_collect_tmp"
        exit 5
    fi
    # Bash 3.2 nounset treats an empty array as unset; preserve zero arguments.
    if ! python3 - "${_collect_tmp}/out" "$JOB_ID" ${_destinations[@]+"${_destinations[@]}"} <<'PYINSTALL'
import hashlib
import json
import os
import re
import sys
import tarfile
import tempfile
from pathlib import Path

root = Path(sys.argv[1])
try:
    files = {}
    artifact_keys = {"result-out", "debug-out", "stream-out", "selfverify-out", "uncommitted-out", "phases-out", "provenance-out"}
    has_manifest = (root / "artifacts.json").exists()
    if has_manifest:
        manifest = json.loads((root / "artifacts.json").read_text())
        if not isinstance(manifest, dict):
            raise ValueError("invalid artifact manifest")
        if "error" in manifest:
            error = manifest["error"]
            if isinstance(error, str) and re.fullmatch(r"[a-z_]{1,64}", error):
                print("remote_agent: " + error, file=sys.stderr)
                sys.exit(5)
            raise ValueError("invalid artifact manifest")
        if "job_id" not in manifest:
            raise ValueError("invalid artifact manifest")
        if manifest["job_id"] != sys.argv[2] or not isinstance(manifest["files"], dict):
            raise ValueError("invalid artifact identity")
        for key, digest in manifest["files"].items():
            if key not in artifact_keys:
                raise ValueError("invalid artifact name")
            data = (root / "artifacts" / key).read_bytes()
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("artifact digest mismatch")
            files[key] = data
    # The shell synthesizes an empty stdout for legacy stream-only collection.
    # A requested --out requires stdout to have actually arrived in the bundle.
    if "--out" in sys.argv[3::2]:
        with tarfile.open(root.parent / "bundle.tar") as bundle:
            if "stdout" not in bundle.getnames():
                raise ValueError("requested artifact unavailable: out")
    files["out"] = (root / "stdout").read_bytes()
    staged = []
    skipped = []
    try:
        for flag, destination in zip(sys.argv[3::2], sys.argv[4::2]):
            key = flag[2:]
            target = Path(destination)
            if key not in files:
                if has_manifest and key in artifact_keys:
                    skipped.append((key, target))
                    continue
                raise ValueError("requested artifact unavailable: " + key)
            fd, temporary = tempfile.mkstemp(prefix=".wb-collect-", dir=target.parent)
            staged.append((temporary, target))
            with os.fdopen(fd, "wb") as stream:
                stream.write(files[key])
                stream.flush()
                os.fsync(stream.fileno())
        # Defer stale-destination removal until validation and staging succeed.
        # An artifact this job did not produce must not leave an earlier job's bytes.
        for key, target in skipped:
            target.unlink(missing_ok=True)
        for temporary, target in staged:
            os.replace(temporary, target)
        for key, target in skipped:
            print("remote_agent: no " + key + " artifact collected", file=sys.stderr)
    finally:
        for temporary, target in staged:
            Path(temporary).unlink(missing_ok=True)
except Exception as exc:
    print("remote_agent: artifact_collection_failed " + str(exc), file=sys.stderr)
    sys.exit(5)
PYINSTALL
    then
        rm -rf "$_collect_tmp"
        exit 5
    fi
    if [ -n "$STAGE_DIR" ]; then
        if ! cp -R "${_collect_tmp}/out/." "$STAGE_DIR/"; then
            echo "remote_agent: collect_stage_failed" >&2
            rm -rf "$_collect_tmp"
            exit 5
        fi
        rm -rf "$_collect_tmp"
        echo "remote_agent: collect_staged rc=${_recorded_rc}" >&2
        exit 0
    fi
    # D3B observation: settle the existing raw reservation after authenticated
    # bundle validation. Engine-owned jobs defer to completion's canonical harvest.
    _review_cycle_bridge collect "$JOB_ID" "${_collect_tmp}/out" "$_recorded_rc" || true
    _stamp_rc=0
    # shellcheck disable=SC2029
    "${SSH[@]}" env \
        "AGENT_ROOT=${AGENT_ROOT}" \
        "JOB_ID=${JOB_ID}" \
        "COLLECT_MODE=stamp" \
        "REMOTE_JOB_OP=collect" \
        bash -s <<COLLECT_STAMP_EOF || _stamp_rc=$?
set -euo pipefail
${_JOB_OPS_SRC}
_remote_job_ops
COLLECT_STAMP_EOF
    if [ "$_stamp_rc" -ne 0 ]; then
        echo "remote_agent: collected_at_stamp_failed rc=${_stamp_rc}" >&2
        rm -rf "$_collect_tmp"
        exit 5
    fi
    rm -rf "$_collect_tmp"
    if [ "$_collect_analysis_refused" = 1 ]; then
        echo "remote_agent: review_cycle_analysis refused: analysis_mutated_source" >&2
        exit 76
    fi
    case "${_recorded_rc}" in
        ''|*[!0-9-]*) exit 0 ;;
        *) exit "${_recorded_rc}" ;;
    esac
    ;;
cancel)
    exec </dev/null
    JOB_ID=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --job-id)
                JOB_ID="${2:-}"
                shift 2
                ;;
            *) die "unknown arg: $1" ;;
        esac
    done
    [ -n "$JOB_ID" ] || die "--job-id required"
    if ! [[ "$JOB_ID" =~ ^j[0-9a-f]{20}$ ]]; then
        die "invalid --job-id (need j + 20 lowercase hex)"
    fi
    _JOB_OPS_SRC="$(declare -f _lane_scope_cgroup_members _remote_job_ops)"
    _cancel_rc=0
    # shellcheck disable=SC2029
    "${SSH[@]}" env \
        "AGENT_ROOT=${AGENT_ROOT}" \
        "JOB_ID=${JOB_ID}" \
        "REMOTE_JOB_OP=cancel" \
        bash -s <<CANCEL_EOF || _cancel_rc=$?
set -euo pipefail
${_JOB_OPS_SRC}
_remote_job_ops
CANCEL_EOF
    if [ "$_cancel_rc" -ne 0 ]; then
        echo "remote_agent: cancel_failed rc=${_cancel_rc}" >&2
        exit "$_cancel_rc"
    fi
    ;;
# BEGIN BUILD_DISPATCH
submit|build)
    # implementation note / decision 4134 (RES-13 crumple zone): close inherited stdin.
    # The orchestrator spawns this script with the MCP server's own stdin — the
    # JSON-RPC stdio pipe, a non-tty, never-EOF fd. Without this, the step-1
    # `git push` below (git's default ssh) blocks reading it forever, burning the
    # whole timeout with no VM sandbox and 0 grok output. All real input arrives
    # via --brief/--schema files and ssh heredocs (which set their own stdin), so
    # /dev/null is safe and only removes the block — robust regardless of caller.
    exec </dev/null
    BRANCH="" BRIEF="" SCHEMA="" OUT="" RESULT_OUT="" DEBUG_OUT="" STREAM_OUT="" TIMEOUT="0" SELFVERIFY_CMD="" SELFVERIFY_OUT="" PHASES_OUT="" UNCOMMITTED_OUT="" PROVENANCE_OUT=""
    AGENT_SPEC=""
    REVIEW_KIND="implement" REVIEW_SUBJECT_BRANCH="" CANON_PROPOSAL_PATH=""
    JOB_ID=""
    AGENT_SPEC_ENABLED=0
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="/dev/null" AGENT_SPEC_STDOUT="{result_file}"
    AGENT_SPEC_STDERR="{run_log}" AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="9"
    AGENT_SPEC_ENV_FILE="" AGENT_SPEC_BRANCH="" AGENT_SPEC_HEAD_SHA=""
    AGENT_SPEC_ENV_COUNT="0" _AGENT_SPEC_ENV_BLOCK=""
    AGENT_SPEC_ENV_EXPORTED_NAMES="" AGENT_SPEC_ENV_UNSET_NAMES=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --review-kind) REVIEW_KIND="${2:-}"; shift 2 ;;
            --review-subject-branch) REVIEW_SUBJECT_BRANCH="${2:-}"; shift 2 ;;
            --canon-proposal-path) CANON_PROPOSAL_PATH="${2:-}"; shift 2 ;;
            --branch) BRANCH="${2:-}"; shift 2 ;;
            --brief) BRIEF="${2:-}"; shift 2 ;;
            --schema) SCHEMA="${2:-}"; shift 2 ;;
            --out) OUT="${2:-}"; shift 2 ;;
            --result-out) RESULT_OUT="${2:-}"; shift 2 ;;
            --debug-out) DEBUG_OUT="${2:-}"; shift 2 ;;
            --stream-out) STREAM_OUT="${2:-}"; shift 2 ;;
            --timeout) TIMEOUT="${2:-}"; shift 2 ;;
            --test-cmd) SELFVERIFY_CMD="${2:-}"; shift 2 ;;
            --selfverify-out) SELFVERIFY_OUT="${2:-}"; shift 2 ;;
            --phases-out) PHASES_OUT="${2:-}"; shift 2 ;;
            --uncommitted-out) UNCOMMITTED_OUT="${2:-}"; shift 2 ;;
            --provenance-out) PROVENANCE_OUT="${2:-}"; shift 2 ;;
            --agent-spec) AGENT_SPEC="${2:-}"; shift 2 ;;
            --job-id) JOB_ID="${2:-}"; shift 2 ;;
            --model|--max-turns|--effort)
                die "--agent-spec required; legacy $1 is removed (implementation note S4)"
                ;;
            *) die "unknown arg: $1" ;;
        esac
    done
    if [ "$cmd" = submit ]; then
        [ -n "$JOB_ID" ] || die "--job-id required"
        if ! [[ "$JOB_ID" =~ ^j[0-9a-f]{20}$ ]]; then
            die "invalid --job-id (need j + 20 lowercase hex)"
        fi
    fi
    [ -n "$BRANCH" ] || die "--branch required"
    [ -f "$BRIEF" ] || die "--brief file not found: ${BRIEF:-<unset>}"
    [ -f "$SCHEMA" ] || die "--schema file not found: ${SCHEMA:-<unset>}"
    # implementation note S4: --agent-spec is mandatory; legacy no-spec grok path deleted.
    [ -n "$AGENT_SPEC" ] || die "--agent-spec required"
    [ -f "$AGENT_SPEC" ] || die "--agent-spec file not found: ${AGENT_SPEC}"
    case "$AGENT_SPEC" in
        *.json) AGENT_SPEC_ARGV="${AGENT_SPEC%.json}.argv" ;;
        *) AGENT_SPEC_ARGV="${AGENT_SPEC}.argv" ;;
    esac
    [ -f "$AGENT_SPEC_ARGV" ] || die "--agent-spec argv sidecar not found: ${AGENT_SPEC_ARGV}"
    # S3-M01: require non-empty argv sidecar ending in NUL before dispatch.
    # A truncated final element (no trailing NUL) would silently drop a flag.
    [ -s "$AGENT_SPEC_ARGV" ] || die "--agent-spec argv sidecar empty: ${AGENT_SPEC_ARGV}"
    _argv_last_hex=$(tail -c 1 "$AGENT_SPEC_ARGV" | od -An -tx1 | tr -d ' \n')
    [ "$_argv_last_hex" = "00" ] || die "--agent-spec argv sidecar missing trailing NUL: ${AGENT_SPEC_ARGV}"
    # Sidecar is the argv that runs; JSON argv is the recorded copy. Refuse
    # before dispatch when they diverge (stale copy or hand-edit of one side).
    if ! python3 - "$AGENT_SPEC" "$AGENT_SPEC_ARGV" <<'PY'
import json, sys

json_path, argv_path = sys.argv[1], sys.argv[2]
spec = json.load(open(json_path, encoding="utf-8"))
json_argv = spec.get("argv")
raw = open(argv_path, "rb").read()
if not raw.endswith(b"\0"):
    print(
        f"--agent-spec argv sidecar missing trailing NUL: {argv_path}",
        file=sys.stderr,
    )
    sys.exit(2)
body = raw[:-1]
try:
    side = [] if body == b"" else [p.decode("utf-8") for p in body.split(b"\0")]
except UnicodeDecodeError:
    print(
        f"--agent-spec argv mismatch at index=0: json={json_path} sidecar={argv_path}",
        file=sys.stderr,
    )
    sys.exit(2)
if not isinstance(json_argv, list):
    print(
        f"--agent-spec argv mismatch at index=0: json={json_path} sidecar={argv_path}",
        file=sys.stderr,
    )
    sys.exit(2)
n = max(len(json_argv), len(side))
for i in range(n):
    left = json_argv[i] if i < len(json_argv) else None
    right = side[i] if i < len(side) else None
    if left != right:
        print(
            f"--agent-spec argv mismatch at index={i}: json={json_path} sidecar={argv_path}",
            file=sys.stderr,
        )
        sys.exit(2)
PY
    then
        die "--agent-spec argv mismatch (see stderr)"
    fi
    # Host parses JSON metadata (bash never parses JSON on the remote side).
    # Values are host-expanded into the unquoted heredoc inside single quotes,
    # so refuse anything that is not whole-token safe before emit [WEB-02].
    # NUL-delimited value sidecar + fixed-position read (HARM-H04).
    # bash 3.2 host: no mapfile, no associative arrays, no ${var^^}.
    _meta_tmp=$(mktemp "${TMPDIR:-/tmp}/ra-agent-spec-meta.XXXXXX") \
        || die "mktemp failed for agent-spec metadata"
    if ! python3 - "$AGENT_SPEC" "$_meta_tmp" <<'PY'
import json, re, sys

spec = json.load(open(sys.argv[1], encoding="utf-8"))
# HARM-M01: version fence before any field copy or emit.
_sv = spec.get("spec_version")
if _sv != 2:
    print(
        f"--agent-spec policy refused: spec_version={_sv!r} (require 2)",
        file=sys.stderr,
    )
    sys.exit(2)

_BIN_RE = re.compile(r"^[A-Za-z0-9._+/-]+$")
_PATH_SEG_RE = re.compile(r"^[A-Za-z0-9._+/-]+$")
_REDIRECT_OK = {
    "/dev/null",
    "{brief_file}",
    "{schema_file}",
    "{result_file}",
    "{stream_file}",
    "{run_log}",
    "{debug_file}",
    "{out_dir}",
}
_ABS_REDIRECT_RE = re.compile(r"^/[A-Za-z0-9._+/-]+$")

_out = open(sys.argv[2], "wb")


def emit(val):
    """Write one metadata VALUE as a NUL-terminated record (no shell syntax)."""
    raw = str(val).encode("utf-8")
    if b"\0" in raw:
        refuse("agent-spec metadata value contains NUL")
    _out.write(raw + b"\0")


def refuse(msg):
    print(msg, file=sys.stderr)
    sys.exit(2)


binary = str(spec.get("binary") or "")
if not binary or not _BIN_RE.fullmatch(binary):
    refuse(f"--agent-spec binary not whole-token safe: {binary!r}")

for field in ("stdin", "stdout", "stderr"):
    raw = spec.get(field)
    val = "/dev/null" if field == "stdin" and not raw else (raw or {
        "stdout": "{result_file}",
        "stderr": "{run_log}",
    }.get(field, ""))
    val = str(val)
    if val not in _REDIRECT_OK and not _ABS_REDIRECT_RE.fullmatch(val):
        refuse(f"--agent-spec {field} not allowed: {val!r}")

pp = spec.get("path_prepend") or []
if not isinstance(pp, list):
    refuse("--agent-spec path_prepend must be a list")
for seg in pp:
    s = str(seg)
    if not s or not _PATH_SEG_RE.fullmatch(s):
        refuse(f"--agent-spec path_prepend segment not whole-token safe: {s!r}")

fds = spec.get("close_fds") or [9]
if not isinstance(fds, list) or any(not isinstance(x, int) or x < 0 for x in fds):
    refuse("--agent-spec close_fds must be a list of non-negative ints")

# Fixed field order — bash assigns by known position only.
emit(binary)
emit(str(spec.get("stdin") or "/dev/null"))
emit(str(spec.get("stdout") or "{result_file}"))
emit(str(spec.get("stderr") or "{run_log}"))
emit(":".join(str(p) for p in pp))
emit(" ".join(str(x) for x in fds))
emit("1" if spec.get("requires_timeout") else "0")
# env_file: optional credential path (implementation note D6). Re-validate host-side.
# Keep ~/ form; remote expands tilde (host single-quote embed cannot expand $HOME).
_ENV_FILE_RE = re.compile(r"^~?/[A-Za-z0-9._+/-]+$")
_ef_raw = spec.get("env_file")
if _ef_raw is None or _ef_raw == "":
    emit("")
else:
    _ef = str(_ef_raw)
    if "\0" in _ef or not _ENV_FILE_RE.fullmatch(_ef):
        refuse(f"--agent-spec env_file not whole-token safe: {_ef!r}")
    emit(_ef)
# env is a counted NUL-delimited tail after the ten fixed metadata records.
# Bash reads each validated NAME/VALUE pair as two records; values never enter
# shell syntax until the grammar below has restricted them to safe whole tokens.
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_ENV_LITERAL_RE = re.compile(r"^[A-Za-z0-9._+/:=@-]{1,256}$")
_ENV_OUT_DIR_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._+-]+$")
_ENV_FORBIDDEN = {
    "PATH",
    "HOME",
    "IFS",
    "ENV",
    "BASH_ENV",
    "SHELLOPTS",
    "BASHOPTS",
    "PS4",
    "VIRTUAL_ENV",
    "UV_CACHE_DIR",
    "PYTHONPATH",
}
_env = spec.get("env", {})
if not isinstance(_env, dict):
    refuse("--agent-spec env must be an object")
for _name, _value in _env.items():
    if (
        not isinstance(_name, str)
        or not _ENV_NAME_RE.fullmatch(_name)
        or _name in _ENV_FORBIDDEN
        or _name.startswith(("LD_", "DYLD_", "BASH_FUNC"))
    ):
        refuse(f"--agent-spec env name not allowed: {_name!r}")
    _value_ok = isinstance(_value, str) and (
        _value == ""
        or _ENV_LITERAL_RE.fullmatch(_value)
        or _value == "{out_dir}"
    )
    if isinstance(_value, str) and _value.startswith("{out_dir}/"):
        _segments = _value[len("{out_dir}/") :].split("/")
        _value_ok = all(
            _segment not in ("", ".", "..")
            and _ENV_OUT_DIR_SEGMENT_RE.fullmatch(_segment)
            for _segment in _segments
        )
    if not _value_ok:
        refuse(f"--agent-spec env value not whole-token safe for {_name}")
if len(_env) > 32:
    _overflow_name = list(_env)[32]
    refuse(f"--agent-spec env has more than 32 entries: {_overflow_name}")
# The adapter stamps these once before the first attempt. They are the single
# dispatch identity and must not be re-derived from a branch that can move.
_branch = str(spec.get("branch") or "")
if not re.fullmatch(r"[A-Za-z0-9/_.-]+", _branch):
    refuse("--agent-spec branch missing or unsafe")
_head_sha = str(spec.get("head_sha") or "")
if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", _head_sha):
    refuse("--agent-spec head_sha missing or invalid")
emit(_branch)
emit(_head_sha)
emit(len(_env))
for _name, _value in _env.items():
    emit(_name)
    emit(_value)
_out.close()
PY
    then
        rm -f "$_meta_tmp"
        die "--agent-spec metadata rejected (see stderr)"
    fi
    # Fixed-position NUL load: eleven fixed fields, then env_count NAME/VALUE pairs.
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="" AGENT_SPEC_STDOUT="" AGENT_SPEC_STDERR=""
    AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="" AGENT_SPEC_REQUIRES_TIMEOUT=""
    AGENT_SPEC_ENV_FILE="" AGENT_SPEC_BRANCH="" AGENT_SPEC_HEAD_SHA=""
    AGENT_SPEC_ENV_COUNT="" _AGENT_SPEC_ENV_BLOCK=""
    AGENT_SPEC_ENV_EXPORTED_NAMES="" AGENT_SPEC_ENV_UNSET_NAMES=""
    # Ten fixed records precede the counted environment tail: binary, redirects,
    # path/fd/timeout, env_file, branch, head_sha, then env_count and NAME/VALUE pairs.
    _meta_i=0
    while [ "$_meta_i" -lt 11 ] && IFS= read -r -d '' _mv; do
        case "$_meta_i" in
            0) AGENT_SPEC_BIN="$_mv" ;;
            1) AGENT_SPEC_STDIN="$_mv" ;;
            2) AGENT_SPEC_STDOUT="$_mv" ;;
            3) AGENT_SPEC_STDERR="$_mv" ;;
            4) AGENT_SPEC_PATH_PREPEND="$_mv" ;;
            5) AGENT_SPEC_CLOSE_FDS="$_mv" ;;
            6) AGENT_SPEC_REQUIRES_TIMEOUT="$_mv" ;;
            7) AGENT_SPEC_ENV_FILE="$_mv" ;;
            8) AGENT_SPEC_BRANCH="$_mv" ;;
            9) AGENT_SPEC_HEAD_SHA="$_mv" ;;
            10) AGENT_SPEC_ENV_COUNT="$_mv" ;;
            *)
                rm -f "$_meta_tmp"
                die "--agent-spec metadata: invalid fixed field"
                ;;
        esac
        _meta_i=$((_meta_i + 1))
    done < "$_meta_tmp"
    [ "$_meta_i" -eq 11 ] || { rm -f "$_meta_tmp"; die "--agent-spec metadata field count ${_meta_i} (need 11)"; }
    case "$AGENT_SPEC_ENV_COUNT" in
        ''|*[!0-9]*) rm -f "$_meta_tmp"; die "--agent-spec env count invalid" ;;
    esac
    [ "$AGENT_SPEC_ENV_COUNT" -le 32 ] || { rm -f "$_meta_tmp"; die "--agent-spec env count exceeds 32"; }
# BEGIN AGENT_SPEC_ENV_TAIL_LOAD
    _agent_spec_env_file_path="$AGENT_SPEC_ENV_FILE"
    _AGENT_SPEC_ENV_BLOCK="_envf='$_agent_spec_env_file_path'
case \"\$_envf\" in
    '~/'*) _envf=\"\$HOME/\${_envf#\\~/}\" ;;
esac
if [ -n \"\$_envf\" ] && [ -f \"\$_envf\" ]; then
    set -a
    # shellcheck disable=SC1090
    . \"\$_envf\"
    set +a
fi
"
    exec 3< "$_meta_tmp"
    _meta_i=0
    while [ "$_meta_i" -lt 11 ]; do
        IFS= read -r -d '' -u 3 _mv || { exec 3<&-; rm -f "$_meta_tmp"; die "--agent-spec metadata sidecar truncated"; }
        _meta_i=$((_meta_i + 1))
    done
    _env_pair_i=0
    while [ "$_env_pair_i" -lt "$AGENT_SPEC_ENV_COUNT" ]; do
        IFS= read -r -d '' -u 3 _env_name || { exec 3<&-; rm -f "$_meta_tmp"; die "--agent-spec env sidecar truncated"; }
        IFS= read -r -d '' -u 3 _env_value || { exec 3<&-; rm -f "$_meta_tmp"; die "--agent-spec env sidecar truncated"; }
        if [ -z "$_env_value" ]; then
            _AGENT_SPEC_ENV_BLOCK="${_AGENT_SPEC_ENV_BLOCK}unset '$_env_name'
"
            if [ -n "$AGENT_SPEC_ENV_UNSET_NAMES" ]; then
                AGENT_SPEC_ENV_UNSET_NAMES="$AGENT_SPEC_ENV_UNSET_NAMES $_env_name"
            else
                AGENT_SPEC_ENV_UNSET_NAMES="$_env_name"
            fi
        else
            case "$_env_value" in
                '{out_dir}')
                    _AGENT_SPEC_ENV_BLOCK="${_AGENT_SPEC_ENV_BLOCK}export '$_env_name'=\"\$OUT_DIR\"
"
                    ;;
                '{out_dir}/'*)
                    _env_suffix="${_env_value#\{out_dir\}}"
                    _AGENT_SPEC_ENV_BLOCK="${_AGENT_SPEC_ENV_BLOCK}export '$_env_name'=\"\$OUT_DIR\"'$_env_suffix'
"
                    ;;
                *)
                    _AGENT_SPEC_ENV_BLOCK="${_AGENT_SPEC_ENV_BLOCK}export '$_env_name'='$_env_value'
"
                    ;;
            esac
            if [ -n "$AGENT_SPEC_ENV_EXPORTED_NAMES" ]; then
                AGENT_SPEC_ENV_EXPORTED_NAMES="$AGENT_SPEC_ENV_EXPORTED_NAMES $_env_name"
            else
                AGENT_SPEC_ENV_EXPORTED_NAMES="$_env_name"
            fi
        fi
        _env_pair_i=$((_env_pair_i + 1))
    done
    _meta_extra=""
    if IFS= read -r -d '' -u 3 _meta_extra || [ -n "$_meta_extra" ]; then
        exec 3<&-
        rm -f "$_meta_tmp"
        die "--agent-spec metadata: too many tail fields"
    fi
    exec 3<&-
    rm -f "$_meta_tmp"
    # Build a shell fragment only from the validated path, names, and values.
    # It is inserted as AGENT_SPEC_ENV_FILE in the unquoted remote heredoc.
    if [ "$AGENT_SPEC_ENV_COUNT" -gt 0 ]; then
        _AGENT_SPEC_ENV_BLOCK="${_AGENT_SPEC_ENV_BLOCK}echo 'remote_agent: agent_spec_env exported=${AGENT_SPEC_ENV_EXPORTED_NAMES} unset=${AGENT_SPEC_ENV_UNSET_NAMES}' >&2
"
    fi
    AGENT_SPEC_ENV_FILE="$_AGENT_SPEC_ENV_BLOCK"
# END AGENT_SPEC_ENV_TAIL_LOAD
    [ -n "${AGENT_SPEC_BIN:-}" ] || die "--agent-spec missing binary"
    AGENT_SPEC_ENABLED=1
    case "$BRANCH" in *[!A-Za-z0-9/_.-]*) die "unsafe --branch name" ;; esac
    case "$TIMEOUT" in *[!0-9]*|"") die "--timeout must be a non-negative integer (seconds; 0=none)" ;; esac
    # implementation note S6 / R2-H08: requires_timeout backends refuse --timeout 0 (exit 7).
    if [ "${AGENT_SPEC_REQUIRES_TIMEOUT:-0}" = 1 ] && [ "$TIMEOUT" -le 0 ]; then
        echo "remote_agent: policy refused: requires_timeout but --timeout is ${TIMEOUT} (need positive bound)" >&2
        exit 7
    fi
    # Separate model-process clock (internal): AgentSpec.lane_timeout_s
    # bounds ONLY the model subprocess while --timeout stays the whole body
    # (setup + model + self-verify + harvest). 0 means no separate clock
    # (turn-bounded families). Refuse a missing clock on a requires_timeout
    # spec, or any non-integer, before spend: a malformed clock must never
    # widen the model's window [AGT-10]. The VM re-reads its shipped copy.
    if ! python3 - "$AGENT_SPEC" "${AGENT_SPEC_REQUIRES_TIMEOUT:-0}" <<'PY'
import json, sys

spec = json.load(open(sys.argv[1], encoding="utf-8"))
requires_timeout = sys.argv[2] == "1"
value = spec.get("lane_timeout_s")
if value is None and not requires_timeout:
    value = 0
if (
    isinstance(value, bool)
    or not isinstance(value, int)
    or not 0 <= value <= 86400
    or (requires_timeout and value == 0)
):
    print(f"remote_agent: policy refused: invalid model clock lane_timeout_s={value!r}", file=sys.stderr)
    sys.exit(7)
PY
    then
        exit 7
    fi
    # Analysis is a read-only classification role, never a source, approval or
    # budget refund. The host checks the executed argv before reservation:
    # Claude only with a read-only --tools set, Codex only in its read-only
    # sandbox, on the matching backend, against the unchanged source HEAD.
    # The ledger, not this caller, chooses the required model/effort.
    if [ "$REVIEW_KIND" = analysis ]; then
        _analysis_refusal=""
        if [ -n "$SELFVERIFY_CMD" ] || [ -n "$CANON_PROPOSAL_PATH" ] || [ -n "$UNCOMMITTED_OUT" ]; then
            _analysis_refusal="analysis_options_not_read_only"
        elif ! _analysis_refusal="$(python3 - "$AGENT_SPEC" "$(git rev-parse HEAD 2>/dev/null)" <<'PY'
import json, sys

spec = json.load(open(sys.argv[1], encoding="utf-8"))
argv = spec.get("argv")
if not isinstance(argv, list) or not all(isinstance(item, str) for item in argv):
    print("analysis_spec_invalid")
    sys.exit(0)


def values(*flags):
    return [argv[i + 1] for i, item in enumerate(argv[:-1]) if item in flags]


def options(tokens, switches, scalars, variadic=()):
    # Parse the complete permitted CLI envelope. Unknown options fail closed:
    # a denylist misses aliases, --flag=value, and future widening options.
    aliases = {"-m": "--model", "-p": "--print", "-s": "--sandbox",
               "-c": "--config", "-C": "--cd", "-o": "--output-last-message"}
    parsed = {}
    i = 0
    while i < len(tokens):
        flag, equals, inline = tokens[i].partition("=")
        flag = aliases.get(flag, flag)
        i += 1
        if flag not in switches | scalars | set(variadic):
            raise ValueError("unvalidated option or positional argument")
        if flag in parsed and flag != "--config":
            raise ValueError("repeated singleton option")
        if flag in switches:
            if equals:
                raise ValueError("value on switch")
            items = []
        elif flag in variadic:
            items = [inline] if equals else []
            # Claude --tools <tools...> consumes every value up to an option,
            # including values after an equals-form first value.
            while i < len(tokens) and not tokens[i].startswith("-"):
                items.append(tokens[i])
                i += 1
            if not items:
                raise ValueError("missing variadic value")
        else:
            if equals:
                items = [inline]
            elif i < len(tokens):
                items = [tokens[i]]
                i += 1
            else:
                raise ValueError("missing option value")
            if not items[0] or items[0].startswith("-"):
                raise ValueError("ambiguous option value")
        parsed.setdefault(flag, []).extend(items)
    return parsed


models = values("-m", "--model")
model = models[0] if len(models) == 1 else ""
backend = spec.get("backend_id")
reason = ""
if not sys.argv[2] or spec.get("head_sha") != sys.argv[2]:
    reason = "analysis_source_head_mismatch"
elif backend == "claude-remote" and model.startswith("claude-"):
    try:
        opts = options(argv,
                       {"--print", "--dangerously-skip-permissions", "--strict-mcp-config",
                        "--no-session-persistence"},
                       {"--output-format", "--model", "--effort", "--setting-sources", "--json-schema"},
                       {"--tools"})
        tools = {tool for value in opts.get("--tools", []) for tool in value.replace(",", " ").split()}
        if ("--tools" not in opts or not tools <= {"Read", "Grep", "Glob"}
                or "--strict-mcp-config" not in opts or opts.get("--setting-sources") != ["user"]):
            raise ValueError("analysis tool or settings widening")
    except ValueError:
        reason = "analysis_requires_read_only_tools"
elif backend == "codex-remote" and model.startswith("gpt-"):
    try:
        if argv[:1] != ["exec"] or argv[-1:] != ["-"]:
            raise ValueError("unexpected Codex command or prompt")
        opts = options(argv[1:-1], {"--json", "--ignore-user-config"},
                       {"--cd", "--model", "--config", "--sandbox", "--output-schema",
                        "--output-last-message"})
        if (opts.get("--sandbox") != ["read-only"] or "--ignore-user-config" not in opts
                or opts.get("--cd") != ["."] or opts.get("--output-schema") != ["{schema_file}"]
                or opts.get("--output-last-message") != ["{result_file}"]):
            raise ValueError("analysis sandbox or output widening")
        keys = set()
        for config in opts.get("--config", []):
            key, equals, value = config.partition("=")
            key = key.strip()
            # Only the builder's route and inert workspace-root settings are
            # admitted. Quoted/dotted sandbox keys, MCP and plugin configs fail
            # closed rather than trying to replicate the TOML config merger.
            if (not equals or key in keys or key not in {
                    "model_reasoning_effort", "service_tier", "sandbox_workspace_write.writable_roots"}):
                raise ValueError("unvalidated config override")
            keys.add(key)
            if key == "sandbox_workspace_write.writable_roots" and json.loads(value) != [".git"]:
                raise ValueError("analysis writable-root widening")
    except ValueError:
        reason = "analysis_requires_read_only_tools"
else:
    reason = "analysis_backend_model_mismatch"
print(reason)
PY
)"; then
            _analysis_refusal="analysis_spec_invalid"
        fi
        if [ -n "$_analysis_refusal" ]; then
            echo "remote_agent: review_cycle_analysis refused: ${_analysis_refusal}" >&2
            exit 76
        fi
        ANALYSIS_SOURCE_HEAD="$(git rev-parse HEAD)"
        ANALYSIS_SOURCE_TREE="$(git rev-parse "${ANALYSIS_SOURCE_HEAD}^{tree}")"
    fi
    # D3B: the shared build|submit boundary precedes ALL transport probes.
    REVIEW_SUBJECT_BRANCH="${REVIEW_SUBJECT_BRANCH:-$BRANCH}"
    if [ -z "$JOB_ID" ]; then JOB_ID="j$(od -An -N10 -tx1 /dev/urandom | tr -d ' \n')"; fi
    _review_cycle_bridge reserve "$JOB_ID" "$REVIEW_SUBJECT_BRANCH" "$REVIEW_KIND" "$AGENT_SPEC" "$CANON_PROPOSAL_PATH" || exit 76
    _review_spool="$(git rev-parse --path-format=absolute --git-common-dir)/workbay/review_cycle_dispatch/spool-$(printf '%s' "$JOB_ID" | shasum -a 256 | cut -d ' ' -f 1)"
    mkdir -p "$_review_spool"
    RESULT_OUT="${RESULT_OUT:-$_review_spool/result.json}"
    PROVENANCE_OUT="${PROVENANCE_OUT:-$_review_spool/provenance.json}"
    PHASES_OUT="${PHASES_OUT:-$_review_spool/phases.json}"
    # Off-box self-verify (item 26): base64 the caller's TEST_CMD so arbitrary shell
    # metacharacters survive interpolation into the remote heredoc intact (only
    # [A-Za-z0-9+/=] reaches the interpolation). Decoded + run on the VM below.
    SELFVERIFY_CMD_B64=""
    if [ -n "$SELFVERIFY_CMD" ]; then
        SELFVERIFY_CMD_B64="$(printf '%s' "$SELFVERIFY_CMD" | base64 | tr -d '\n')"
    fi

    # Collision-proof lane key from the FULL branch name [CON-11]: basename-only
    # keys collided (fix_x/fix.x/fix-x; >48-char truncations) and force-pushed /
    # rm -rf'd concurrent sandboxes + systemd unit names. Format:
    #   <sanitized-full-branch-truncated-to-40>-<first-8-of-sha256(exact-full-branch)>
    # so distinct full branch strings never share a key. Leading '-' stripped so
    # the systemd unit name stays valid.
    if command -v shasum >/dev/null 2>&1; then
        BRANCH_HASH="$(printf '%s' "$BRANCH" | shasum -a 256 | awk '{print substr($1,1,8)}')"
    else
        BRANCH_HASH="$(printf '%s' "$BRANCH" | sha256sum | awk '{print substr($1,1,8)}')"
    fi
    LANE_KEY="$(printf '%s' "$BRANCH" | tr -c 'A-Za-z0-9-' '-' | cut -c1-40)"
    while [ "${LANE_KEY#-}" != "$LANE_KEY" ]; do LANE_KEY="${LANE_KEY#-}"; done
    LANE_KEY="${LANE_KEY:-lane}"
    LANE_KEY="${LANE_KEY}-${BRANCH_HASH}"
    # Per-dispatch nonce for TRANSIENT resources (pushed ref, brief, schema,
    # result/debug/selfverify artifacts). LANE_KEY stays shared for the lock-
    # protected sandbox dir and warm venv. Must be unique even for two same-
    # branch dispatches started in the same second; filesystem- and git-ref-safe
    # [CON-02][CON-11][CON-12]. Pid + 8 urandom bytes (date fallback).
    # `|| true` keeps the empty-suffix fallback reachable under set -euo pipefail:
    # without it a missing `od` aborts at assignment and the guard never runs.
    if [[ "${WORKBAY_DISPATCH_NONCE:-}" =~ ^[0-9]+-[0-9a-f]{16}$ ]]; then
        DISPATCH_NONCE="$WORKBAY_DISPATCH_NONCE"
    else
        DISPATCH_NONCE="${$}-$(od -An -N8 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n' || true)"
        # Fallback must keep the ONE system shape <pid>-<16hex> so the ref reaper
        # name guard can match both mint paths [REF-10][RES-07].
        [ -n "${DISPATCH_NONCE#*-}" ] || DISPATCH_NONCE="${$}-$(printf '%08x%08x' "$(date +%s)" "${RANDOM:-0}")"
    fi
    # Named systemd scope unit: workbay-lane-<LANE_KEY>.scope so active lanes are
    # countable + debuggable ([RES-14] concurrency ceiling; implementation note S5).
    # Suffix is part of the name: is-active/reset-failed resolve bare names to
    # .service, but the occupant is created as a scope (systemd-run --scope).
    LANE_UNIT="workbay-lane-${LANE_KEY}.scope"
    LANE_UNIT_SV="workbay-lane-${LANE_KEY}-sv.scope"
    LEGACY_LANE_UNIT="grok-lane-${LANE_KEY}.scope"
    LEGACY_LANE_UNIT_SV="grok-lane-${LANE_KEY}-sv.scope"

    # Wall-clock for residual --timeout after pre-dispatch work (probe/push/scp).
    BUILD_START_TS="$(date +%s)"
    # Per-phase progress with elapsed seconds (implementation note observability delta): a
    # future stall now names its phase + duration instead of a silent timeout.
    _phase() { echo "remote_agent: [+$(( $(date +%s) - BUILD_START_TS ))s] $*" >&2; }
    # implementation note S1: structured host-phase lines (absolute integer Unix seconds).
    # Adapter concurrent stderr reader parses these; cumulative _phase is observational only.
    _emit_host_phase() {
        # $1=name $2=start_ts $3=end_ts
        local _n="$1" _s="$2" _e="$3"
        local _d=$(( _e - _s ))
        echo "remote_agent: phase ${_n} start_ts=${_s} end_ts=${_e} duration_s=${_d}" >&2
    }
    _emit_host_instant() {
        # $1=name (ssh_call_ts | ssh_return_ts) $2=ts
        echo "remote_agent: phase $1 ts=$2" >&2
    }

    # BEGIN SCP_DEADLINE_WRAPPER
    # ConnectTimeout covers only connection establishment.  A connected peer
    # can still stop consuming or producing bytes forever, so every transfer
    # also gets keepalives and a process deadline.  Positive --timeout uses the
    # shared build deadline; timeout=0 still receives a short per-transfer cap.
    _scp_with_deadline() {
        local _now _remaining _budget _stall_cap=120
        _now="$(date +%s)"
        if [ "$TIMEOUT" -gt 0 ]; then
            _remaining=$(( BUILD_START_TS + TIMEOUT - _now ))
            if [ "$_remaining" -le 0 ]; then
                echo 'remote_agent: scp timed out: build deadline exhausted' >&2
                return 124
            fi
            _budget="$_remaining"
            [ "$_budget" -le "$_stall_cap" ] || _budget="$_stall_cap"
        else
            _budget="$_stall_cap"
        fi
        python3 - "$_budget" "$@" <<'PY'
import subprocess
import sys

budget = int(sys.argv[1])
command = [
    "scp", "-q", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2",
    *sys.argv[2:],
]
try:
    raise SystemExit(subprocess.run(command, stdin=subprocess.DEVNULL, timeout=budget).returncode)
except subprocess.TimeoutExpired:
    print(f"remote_agent: scp timed out after {budget}s", file=sys.stderr)
    raise SystemExit(124)
PY
    }
    # END SCP_DEADLINE_WRAPPER

    # BEGIN ADMISSION_PROBE_DEADLINE
    # Bound the pre-dispatch admission ssh independently, and charge it to the
    # shared build deadline when one is configured. Keep stdin inherited so the
    # caller's admission heredoc reaches the remote bash process.
    _admission_probe_with_deadline() {
        local _now _remaining _budget _start _rc=0
        _budget="${WORKBAY_REMOTE_AGENT_ADMISSION_PROBE_TIMEOUT_S-120}"
        if [[ ! "$_budget" =~ ^[0-9]+$ ]] || [ "$_budget" -lt 1 ]; then
            echo "remote_agent: invalid WORKBAY_REMOTE_AGENT_ADMISSION_PROBE_TIMEOUT_S (expected integer >= 1): $_budget" >&2
            return 2
        fi

        _start="$(date +%s)"
        _now="$_start"
        if [ "$TIMEOUT" -gt 0 ]; then
            _remaining=$(( BUILD_START_TS + TIMEOUT - _now ))
            if [ "$_remaining" -lt 1 ]; then
                _budget=1
            elif [ "$_remaining" -lt "$_budget" ]; then
                _budget="$_remaining"
            fi
        fi

        python3 -c '
import subprocess
import sys

budget = int(sys.argv[1])
try:
    raise SystemExit(subprocess.run(sys.argv[2:], timeout=budget).returncode)
except subprocess.TimeoutExpired:
    print(
        f"remote_agent: vm_admission_unmeasured gauge=admission_probe_deadline budget_s={budget}",
        file=sys.stderr,
    )
    raise SystemExit(75)
' "$_budget" "${SSH[@]}" bash -s || _rc=$?
        _emit_host_phase admission "$_start" "$(date +%s)"
        return "$_rc"
    }
    # END ADMISSION_PROBE_DEADLINE

    # Best-effort cleanup of THIS dispatch's nonce'd transients only. Never
    # touches another dispatch's nonce, never runs until we staged our own
    # inputs, never aborts the run or masks the exit code [RES-13][AGT-10].
    _dispatch_staged=0
    _cleanup_dispatch_transients() {
        [ "${_dispatch_staged:-0}" = "1" ] || return 0
        # Non-fatal [RES-13]: still never change the run exit code. Degrade
        # loudly once when the SSH cleanup itself fails (silent || true hid
        # transport failure that left nonce'd refs/files on the VM) [AGT-10].
        # When the remote lane is still live (occupancy lease names THIS
        # dispatch's nonce AND `_lane_occupant_live` is true), spare every
        # transient for the age-based reaper [RES-07] — the remote scope is
        # designed to outlive the local dispatcher, and unlinking its outbox
        # converts a transport kill into total loss of completed work.
        # Occupancy is the same released-aware predicate TTL paths use
        # [REF-26]: `released=1` is a tombstone even with a future expiry.
        # Unparseable expiry/issued still fails open as live. Lease check and
        # removals share one ssh round-trip so there is no test-then-delete
        # race. The per-job submit lock also spans the state decision and the
        # removals, so a remote submit cannot finish its ready -> submitted
        # handoff after cleanup has observed ready.
        if ! "${SSH[@]}" "_agent_root='${AGENT_ROOT}'; case \"\$_agent_root\" in /*) ;; *) _agent_root=\"\$HOME/\$_agent_root\" ;; esac; \
            _lane_occupant_live() { \
                _lk=\"\${1:-}\"; \
                [ -n \"\$_lk\" ] || return 0; \
                _lf=\"\$_agent_root/.lane-live-\$_lk\"; \
                [ -f \"\$_lf\" ] || return 1; \
                _expiry=; _issued=; _released=; \
                while IFS= read -r _lline || [ -n \"\$_lline\" ]; do \
                    case \"\$_lline\" in \
                        expiry=*) _expiry=\$(printf '%s\\n' \"\$_lline\" | sed 's/^expiry=//') ;; \
                        issued=*) _issued=\$(printf '%s\\n' \"\$_lline\" | sed 's/^issued=//') ;; \
                        released=*) _released=\$(printf '%s\\n' \"\$_lline\" | sed 's/^released=//') ;; \
                    esac; \
                done <\"\$_lf\" || return 0; \
                case \"\$_expiry\" in ''|*[!0-9]*) return 0 ;; esac; \
                case \"\$_issued\" in ''|*[!0-9]*) return 0 ;; esac; \
                case \"\$_released\" in \
                    1) return 1 ;; \
                    ''|0) ;; \
                    *) return 0 ;; \
                esac; \
                _now=\$(date +%s); \
                if [ \"\$_now\" -ge \"\$_expiry\" ]; then return 1; fi; \
                return 0; \
            }; \
            _lv=\"\$_agent_root/.lane-live-${LANE_KEY}\"; \
            _live=0; \
            if [ -f \"\$_lv\" ] && grep -qx 'nonce=${DISPATCH_NONCE}' \"\$_lv\" 2>/dev/null; then \
                if _lane_occupant_live \"${LANE_KEY}\"; then _live=1; fi; \
            fi; \
            _handoff=0; \
            _cleanup=1; \
            if [ \"\$_live\" != 1 ] && [ -n \"${JOB_ID:-}\" ]; then \
                _jd=\"\$_agent_root/.jobs/${JOB_ID:-}\"; \
                _jf=\"\$_jd/state.json\"; \
                if ! exec 9>>\"\$_jd/.lock\"; then exit 1; fi; \
                if ! flock -w 5 9; then exit 1; fi; \
                if [ -f \"\$_jf\" ]; then \
                    if ! _st=\$(sed -n 's/.*\"state\":\"\\([^\"]*\\)\".*/\\1/p' \"\$_jf\"); then exit 1; fi; \
                    case \"\$_st\" in \
                        submitted|running|done) _handoff=1 ;; \
                        claiming|ready) _cleanup=1 ;; \
                        *) exit 1 ;; \
                    esac; \
                fi; \
                if [ -f \"\$_jd/done.json\" ] || [ -f \"\$_jd/started\" ]; then _handoff=1; _cleanup=0; fi; \
            fi; \
            if [ \"\$_live\" = 1 ] || [ \"\$_handoff\" = 1 ]; then \
                if [ \"\$_handoff\" = 1 ]; then \
                    echo 'remote_agent: submitted job holds dispatch transients — sparing outbox' >&2; \
                else \
                    echo 'remote_agent: live lane holds this dispatch nonce — sparing outbox for age-based reaper' >&2; \
                fi; \
            elif [ \"\$_cleanup\" = 1 ]; then \
                rm -f \
                \"\$_agent_root/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md\" \
                \"\$_agent_root/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.agent-result.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-result.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-debug.log\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-selfverify.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.agent-phases.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-phases.json\" \
                \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.workbay-source-provenance.json\" \
                \"\$_agent_root/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json\" \
                \"\$_agent_root/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.argv\" \
                2>/dev/null; \
                rmdir \"\$_agent_root/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}\" 2>/dev/null; \
                git -C \"\$HOME/${REMOTE_DIR}\" update-ref -d 'refs/heads/${LANE_KEY}-${DISPATCH_NONCE}' 2>/dev/null; \
            fi; \
            true"; then
            echo 'remote_agent: dispatch transient cleanup failed (non-fatal)' >&2
        fi
        return 0
    }
    _review_cycle_exit() {
        local _review_exit_rc=$?
        if [ "${_review_cycle_finished:-0}" = 0 ] && [ -z "${WORKBAY_REVIEW_CYCLE_TOKEN:-}" ]; then
            local _review_exit_mode=settle
            [ "${_review_model_may_have_started:-0}" = 1 ] || _review_exit_mode=premodel
            _review_cycle_bridge "$_review_exit_mode" "$JOB_ID" "$REVIEW_SUBJECT_BRANCH" "$REVIEW_KIND" "$AGENT_SPEC" "$CANON_PROPOSAL_PATH" "$_review_exit_rc" "$RESULT_OUT" "$PROVENANCE_OUT" "${_review_patch_evidence:-${OUT:-$_review_spool/patch}}" || true
        fi
        _cleanup_dispatch_transients
    }
    trap '_review_cycle_exit' EXIT

    # Single-source admission for pre-dispatch and the in-run recheck.
    # Placeholders contain validated numeric options; MAX_LANES may be empty.
    read -r -d '' _admission_tpl <<'ADMISSION_EOF' || true
pids_root="${WORKBAY_REMOTE_GATE_PIDS_ROOT:-/sys/fs/cgroup/user.slice/user-$(id -u).slice}"
_read_uint() {
    awk 'NR == 1 && NF == 1 && $1 ~ /^[0-9]+$/ {v=$1; ok=1}
         END {if (ok && NR == 1) print v; else exit 1}' "$1" 2>/dev/null
}
_read_ceiling() {
    awk 'NR == 1 && NF == 1 && ($1 == "max" || $1 ~ /^[0-9]+$/) {v=$1; ok=1}
         END {if (ok && NR == 1) print v; else exit 1}' "$1" 2>/dev/null
}
_read_anon() {
    awk '$1 == "anon" {n++; if (NF == 2 && $2 ~ /^[0-9]+$/) {v=$2; ok=1}}
         END {if (ok && n == 1) print v; else exit 1}' "$1" 2>/dev/null
}
_read_psi() {
    awk '$1 == "some" {for(i=2;i<=NF;i++) if ($i ~ /^avg60=/) {
             n++; split($i,a,"="); if(a[2] ~ /^[0-9]+([.][0-9]+)?$/) {v=a[2]; ok=1}}}
         END {if(ok && n == 1) print v; else exit 1}' "$1" 2>/dev/null
}
_read_usage_usec() {
    awk '$1 == "usage_usec" {n++; if (NF == 2 && $2 ~ /^[0-9]+$/) {v=$2; ok=1}}
         END {if (ok && n == 1) print v; else exit 1}' "$1" 2>/dev/null
}
_admission_scope_snapshot() {
    _ass_phase=$1
    _ass_metric=$2
    _ass_candidate_cap=${3:-__ATTRIBUTION_CANDIDATE_CAP__}
    case "$_ass_candidate_cap" in ''|*[!0-9]*) return 3 ;; esac
    _ass_candidate_cap=$((10#$_ass_candidate_cap))
    _ass_candidates=0
    # Unmatched scope globs cannot distinguish absent scopes from hidden ones.
    if [ -d "$pids_root" ]; then
        [ -r "$pids_root" ] && [ -x "$pids_root" ] || return 2
    fi
    for _ass_service in "$pids_root"/user@*.service; do
        [ -d "$_ass_service" ] || continue
        [ -r "$_ass_service" ] && [ -x "$_ass_service" ] || return 2
        _ass_app_slice=$_ass_service/app.slice
        [ -d "$_ass_app_slice" ] || continue
        [ -r "$_ass_app_slice" ] && [ -x "$_ass_app_slice" ] || return 2
    done
    for _ass_scope in "$pids_root"/user@*.service/app.slice/workbay-lane-*.scope \
        "$pids_root"/user@*.service/app.slice/grok-lane-*.scope; do
        [ -d "$_ass_scope" ] || continue
        _ass_candidates=$((_ass_candidates + 1))
        [ "$_ass_candidates" -le "$_ass_candidate_cap" ] || return 3
        _ass_key=${_ass_scope##*/}
        _ass_pids=$(_read_uint "$_ass_scope/pids.current") || return 1
        [ "$_ass_pids" -gt 0 ] || continue
        if [ "$_ass_metric" = cpu ]; then
            _ass_value=$(_read_usage_usec "$_ass_scope/cpu.stat") || return 1
        else
            _ass_value=$(_read_uint "$_ass_scope/memory.current") || return 1
        fi
        printf '%s|%s|%s|%s\n' "$_ass_phase" "$_ass_key" "$_ass_pids" "$_ass_value"
    done
}
# Read once so a timestamp cannot straddle a second boundary between fields.
_admission_now_ms() {
    local _anm_rt="${EPOCHREALTIME:-}" _anm_s _anm_us
    case "$_anm_rt" in *[.,]*) ;; *) return 1 ;; esac
    _anm_s=${_anm_rt%%[.,]*}
    _anm_us=${_anm_rt#*[.,]}
    case "$_anm_s" in ''|*[!0-9]*) return 1 ;; esac
    case "$_anm_us" in ''|*[!0-9]*) return 1 ;; esac
    printf '%s\n' "$(( 10#$_anm_s * 1000 + 10#$_anm_us / 1000 ))"
}
_admission_remaining_timeout() {
    local _art_deadline_ms=$1 _art_now_ms _art_left_ms _art_timeout
    _art_now_ms=$(_admission_now_ms) || return 1
    case "$_art_now_ms" in ''|*[!0-9]*) return 1 ;; esac
    _art_left_ms=$((_art_deadline_ms - _art_now_ms))
    [ "$_art_left_ms" -gt 0 ] || return 1
    # Reserve the forced-kill grace inside the shared attribution deadline.
    _art_left_ms=$((_art_left_ms - _ats_kill_grace_ms))
    [ "$_art_left_ms" -gt 0 ] || _art_left_ms=1
    printf -v _art_timeout '%d.%03d' "$((_art_left_ms / 1000))" "$((_art_left_ms % 1000))"
    printf '%s\n' "$_art_timeout"
}
_admission_scope_snapshot_bounded() {
    local _asb_phase=$1 _asb_metric=$2 _asb_candidate_cap=$3 _asb_timeout=$4 _asb_functions
    command -v timeout >/dev/null 2>&1 || return 124
    _asb_functions=$(declare -f _read_uint _read_usage_usec _admission_scope_snapshot) || return 1
    timeout -k "$_ats_kill_grace" "$_asb_timeout" bash -c "$_asb_functions
pids_root=\$1
_admission_scope_snapshot \"\$2\" \"\$3\" \"\$4\"" \
        attribution-snapshot "$pids_root" "$_asb_phase" "$_asb_metric" "$_asb_candidate_cap"
}
_admission_top_scopes() {
    local _ats_metric=$1 _ats_deadline_raw _ats_whole _ats_fraction _ats_seconds _ats_millis
    local _ats_budget_ms _ats_now_ms _ats_deadline_ms _ats_cap_raw _ats_candidate_cap
    local _ats_timeout _ats_status _ats_before _ats_after
    # Fixed containment grace: even TERM-ignoring workers must be killed.
    local _ats_kill_grace_ms=300 _ats_kill_grace
    printf -v _ats_kill_grace '%d.%03d' "$((_ats_kill_grace_ms / 1000))" "$((_ats_kill_grace_ms % 1000))"
    _ats_deadline_raw=__ATTRIBUTION_DEADLINE_SEC__
    if [[ ! "$_ats_deadline_raw" =~ ^([0-9]{1,3})([.]([0-9]{1,3}))?$ ]]; then
        echo 'attribution: unavailable (deadline)'
        return 0
    fi
    _ats_whole="${BASH_REMATCH[1]}"
    _ats_fraction="${BASH_REMATCH[3]:-000}"
    while [ "${#_ats_fraction}" -lt 3 ]; do _ats_fraction="${_ats_fraction}0"; done
    _ats_seconds=$((10#$_ats_whole))
    _ats_millis=$((10#$_ats_fraction))
    if [ "$_ats_seconds" -gt 300 ] || { [ "$_ats_seconds" -eq 300 ] && [ "$_ats_millis" -gt 0 ]; }; then
        echo 'attribution: unavailable (deadline)'
        return 0
    fi
    _ats_budget_ms=$((_ats_seconds * 1000 + _ats_millis))
    if [ "$_ats_budget_ms" -le 0 ]; then
        echo 'attribution: unavailable (deadline)'
        return 0
    fi
    _ats_cap_raw=__ATTRIBUTION_CANDIDATE_CAP__
    case "$_ats_cap_raw" in ''|*[!0-9]*) echo 'attribution: unavailable (cap)'; return 0 ;; esac
    if [ "${#_ats_cap_raw}" -gt 5 ]; then
        echo 'attribution: unavailable (cap)'
        return 0
    fi
    _ats_candidate_cap=$((10#$_ats_cap_raw))
    if [ "$_ats_candidate_cap" -lt 1 ] || [ "$_ats_candidate_cap" -gt 4096 ]; then
        echo 'attribution: unavailable (cap)'
        return 0
    fi
    _ats_now_ms=$(_admission_now_ms) || {
        echo 'attribution: unavailable (deadline)'
        return 0
    }
    case "$_ats_now_ms" in ''|*[!0-9]*) echo 'attribution: unavailable (deadline)'; return 0 ;; esac
    _ats_deadline_ms=$((_ats_now_ms + _ats_budget_ms))
    _ats_timeout=$(_admission_remaining_timeout "$_ats_deadline_ms") || {
        echo 'attribution: unavailable (deadline)'
        return 0
    }
    _ats_before=$(_admission_scope_snapshot_bounded B "$_ats_metric" "$_ats_candidate_cap" "$_ats_timeout") || {
        _ats_status=$?
        case "$_ats_status" in
            124|137|143) echo 'attribution: unavailable (deadline)' ;;
            3) echo 'attribution: unavailable (cap)' ;;
            2) echo unavailable:scope_dir_unreadable ;;
            *) echo unavailable:metric_read_failed ;;
        esac
        return 0
    }
    if [ -z "$_ats_before" ]; then
        echo none
        return 0
    fi
    if [ "$_ats_metric" = cpu ]; then
        # A failed sample window cannot establish a measured CPU delta.
        _ats_timeout=$(_admission_remaining_timeout "$_ats_deadline_ms") || {
            echo 'attribution: unavailable (deadline)'
            return 0
        }
        if timeout -k "$_ats_kill_grace" "$_ats_timeout" sleep 1 >/dev/null 2>&1; then
            :
        else
            _ats_status=$?
            case "$_ats_status" in
                124|137|143) echo 'attribution: unavailable (deadline)' ;;
                *) echo unavailable:sample_window_failed ;;
            esac
            return 0
        fi
    fi
    _ats_timeout=$(_admission_remaining_timeout "$_ats_deadline_ms") || {
        echo 'attribution: unavailable (deadline)'
        return 0
    }
    _ats_after=$(_admission_scope_snapshot_bounded A "$_ats_metric" "$_ats_candidate_cap" "$_ats_timeout") || {
        _ats_status=$?
        case "$_ats_status" in
            124|137|143) echo 'attribution: unavailable (deadline)' ;;
            3) echo 'attribution: unavailable (cap)' ;;
            2) echo unavailable:scope_dir_unreadable ;;
            *) echo unavailable:metric_read_failed ;;
        esac
        return 0
    }
    printf '%s\n%s\n' "$_ats_before" "$_ats_after" | awk -F '|' -v metric="$_ats_metric" '
        $1 == "B" {before[$2] = $4; before_count++; next}
        $1 == "A" && !($2 in before) {changed = 1; next}
        $1 == "A" && ($2 in before) && $4 ~ /^[0-9]+$/ {
            value = metric == "cpu" ? $4 - before[$2] : $4
            if (value < 0) reset = 1
            count++
            key[count] = $2
            pids[count] = $3
            amount[count] = value
        }
        END {
            if (reset) {print "unavailable:counter_reset"; exit}
            if (changed || count != before_count || count == 0) {
                print "unavailable:scope_changed"; exit
            }
            field = metric == "cpu" ? "cpu_usage_usec_delta" : "memory_current"
            for (slot = 1; slot <= 3 && slot <= count; slot++) {
                best = 0
                for (i = 1; i <= count; i++) {
                    if (used[i]) continue
                    if (best == 0 || amount[i] > amount[best] ||
                        (amount[i] == amount[best] && key[i] < key[best])) best = i
                }
                if (slot > 1) printf ";"
                printf "%s:pids=%s,%s=%.0f", key[best], pids[best], field, amount[best]
                used[best] = 1
            }
            printf "\n"
        }'
}
_admission_unmeasured() {
    _au_metric=cpu
    case "$1" in vm_memory|vm_memory_psi) _au_metric=memory ;; esac
    echo "remote_agent: vm_admission_unmeasured gauge=$1 top_scopes=$(_admission_top_scopes "$_au_metric")" >&2
    exit 75
}
_admission_saturated() {
    _as_metric=cpu
    case "$1" in vm_memory|vm_memory_psi) _as_metric=memory ;; esac
    # Keep the decision, gauge details, and consuming scopes in one event.
    echo "remote_agent: vm_measured_saturation gauge=$1 $2 — deferring lane top_scopes=$(_admission_top_scopes "$_as_metric")" >&2
    exit 75
}
avail_mb=$(awk '/^MemAvailable:/{printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 0)
if [ "$avail_mb" -gt 0 ] && [ "$avail_mb" -lt __MEM_FLOOR_MB__ ]; then
    echo "remote_agent: VM MemAvailable ${avail_mb}MiB < __MEM_FLOOR_MB__MiB floor — deferring lane (non-lane work has priority) top_scopes=$(_admission_top_scopes memory)" >&2
    exit 75
fi
active_lanes=0
if _al_out=$(systemctl --user list-units --type=scope --state=active --plain --no-legend \
    'workbay-lane-*' 'grok-lane-*' 2>/dev/null | awk 'END{print NR+0}'); then
    case "$_al_out" in ''|*[!0-9]*) active_lanes=0 ;; *) active_lanes=$_al_out ;; esac
fi
if [ -n "__MAX_LANES__" ] && [ "$active_lanes" -ge "__MAX_LANES__" ]; then
    echo "remote_agent: lane cap __MAX_LANES__ reached — deferring top_scopes=$(_admission_top_scopes cpu)" >&2
    exit 75
fi
# An absent hierarchy (non-systemd hosts) skips cgroup gauges. A present
# hierarchy with missing/malformed measurements defers: silence is not health.
if [ -d "$pids_root" ]; then
    pids_max=$(_read_ceiling "$pids_root/pids.max") || _admission_unmeasured vm_pids
    pids_cur=$(_read_uint "$pids_root/pids.current") || _admission_unmeasured vm_pids
    memory_max=$(_read_ceiling "$pids_root/memory.max") || _admission_unmeasured vm_memory
    anon=$(_read_anon "$pids_root/memory.stat") || _admission_unmeasured vm_memory
    lane_pids=__LANE_PIDS_FLOOR__
    lane_anon=$((__LANE_ANON_FLOOR_MB__ * 1024 * 1024))
    for scope in "$pids_root"/user@*.service/app.slice/*lane*.scope; do
        [ -d "$scope" ] || continue
        scope_pids=$(_read_uint "$scope/pids.current") || _admission_unmeasured vm_pids
        scope_anon=$(_read_anon "$scope/memory.stat") || _admission_unmeasured vm_memory
        lane_pids=$(awk -v a="$lane_pids" -v b="$scope_pids" 'BEGIN {print (a>b?a:b)}')
        lane_anon=$(awk -v a="$lane_anon" -v b="$scope_anon" 'BEGIN {printf "%.0f", (a>b?a:b)}')
    done
    if [ "$pids_max" != max ] && awk -v cur="$pids_cur" -v cost="$lane_pids" -v cap="$pids_max" 'BEGIN {exit !(cur+cost+__PID_FLOOR__>cap)}'; then
        _admission_saturated vm_pids "current=$pids_cur lane=$lane_pids reserve=__PID_FLOOR__ max=$pids_max"
    fi
    if [ "$memory_max" != max ] && awk -v cur="$anon" -v cost="$lane_anon" -v cap="$memory_max" 'BEGIN {exit !(cur+cost>cap*__HEADROOM_PCT__/100)}'; then
        _admission_saturated vm_memory "anon=$anon lane=$lane_anon max=$memory_max headroom_pct=__HEADROOM_PCT__"
    fi
    mem_psi=$(_read_psi "$pids_root/memory.pressure") || _admission_unmeasured vm_memory_psi
    cpu_psi=$(_read_psi "$pids_root/cpu.pressure") || _admission_unmeasured vm_cpu_psi
    if awk -v p="$mem_psi" 'BEGIN {exit !(p>__MEM_PSI_MAX__)}'; then
        _admission_saturated vm_memory_psi "avg60=$mem_psi max=__MEM_PSI_MAX__"
    fi
    if awk -v p="$cpu_psi" 'BEGIN {exit !(p>__CPU_PSI_MAX__)}'; then
        _admission_saturated vm_cpu_psi "avg60=$cpu_psi max=__CPU_PSI_MAX__"
    fi
fi
# Probe the nearest existing parent on first dispatch, before the agent root is
# created. It lives on the same filesystem that will hold the new directory.
_disk_probe="${HOME:-.}/__AGENT_ROOT__"
while [ ! -d "$_disk_probe" ] && [ "$_disk_probe" != / ] && [ "$_disk_probe" != . ]; do
    _disk_probe=$(dirname "$_disk_probe")
done
_disk_kb=$(df -Pk "$_disk_probe" 2>/dev/null | awk 'NR==2 && $4 ~ /^[0-9]+$/ {print $4; ok=1} END {if(!ok) exit 1}') || _admission_unmeasured vm_disk
if awk -v kb="$_disk_kb" 'BEGIN {exit !(kb*1024<__DISK_FLOOR_GB__*1024*1024*1024)}'; then
    _admission_saturated vm_disk "free_kb=$_disk_kb floor_gb=__DISK_FLOOR_GB__"
fi
# COST-06 / OBS-08: the pressured-sweep receipt is advisory. The dispatch-local
# maintenance sweep runs after admission, so a missing or ineffective receipt
# must warn and continue; refusing here would create a self-sustaining outage
# because only standalone `reap` writes `.reap-last.json`.
if [ -n "${HOME:-}" ]; then
    _disk_root="$HOME/__AGENT_ROOT__"
    _pressure_pct="${WORKBAY_REMOTE_AGENT_DISK_PRESSURE_PCT:-85}"
    case "$_pressure_pct" in *[!0-9]*|"") _pressure_pct=85 ;; esac
    _used_pct=$(df -P "$_disk_root" 2>/dev/null | awk 'NR==2 { gsub(/%/,"",$5); print $5+0 }')
    case "$_used_pct" in ''|*[!0-9]*) _used_pct=0 ;; esac
    _last="$_disk_root/.reap-last.json"
    if [ "$_used_pct" -ge "$_pressure_pct" ]; then
        if [ ! -s "$_last" ]; then
            echo "remote_agent: vm_disk_pressure_reap_unattested — disk is still ${_used_pct}% (threshold ${_pressure_pct}%) and no sweep receipt is available — admission degraded; proceeding so the dispatch-local sweep can run" >&2
        else
            _last_pressured=$(sed -n 's/.*"pressured":[[:space:]]*\([^,}]*\).*/\1/p' "$_last" | head -n1)
            _last_freed=$(sed -n 's/.*"freed_kb":[[:space:]]*\([^,}]*\).*/\1/p' "$_last" | head -n1)
            case "$_last_freed" in ''|*[!0-9]*) _last_freed=1 ;; esac
            case "$_last_pressured" in
                true|1)
                    if [ "$_last_freed" -eq 0 ]; then
                        echo "remote_agent: vm_disk_pressure_reap_ineffective — last pressured sweep freed 0 kb and disk is still ${_used_pct}% (threshold ${_pressure_pct}%) — admission degraded; proceeding so the dispatch-local sweep can run" >&2
                    fi
                    ;;
            esac
        fi
    fi
fi
ADMISSION_EOF
    # bash 3.2 makes these replacements quadratic (9.4s in C; 124s in UTF-8);
    # bash 5.2 also expands & in replacement values.
    # Replace in argv bytes and write bytes, so no inherited stdout codec can refuse the template's em dashes.
    if ! _admission_remote_sh="$(python3 -c '
import os
import sys

args = [os.fsencode(arg) for arg in sys.argv[1:]]
rendered = args[0]
for index in range(1, len(args), 2):
    rendered = rendered.replace(args[index], args[index + 1])
sys.stdout.buffer.write(rendered)
' "$_admission_tpl" \
        "__MEM_FLOOR_MB__" "$MEM_FLOOR_MB" \
        "__MAX_LANES__" "$MAX_LANES" \
        "__PID_FLOOR__" "$PID_FLOOR" \
        "__HEADROOM_PCT__" "$HEADROOM_PCT" \
        "__MEM_PSI_MAX__" "$MEM_PSI_MAX" \
        "__CPU_PSI_MAX__" "$CPU_PSI_MAX" \
        "__DISK_FLOOR_GB__" "$DISK_FLOOR_GB" \
        "__LANE_PIDS_FLOOR__" "$LANE_PIDS_FLOOR" \
        "__LANE_ANON_FLOOR_MB__" "$LANE_ANON_FLOOR_MB" \
        "__ATTRIBUTION_DEADLINE_SEC__" "$ATTRIBUTION_DEADLINE_SEC" \
        "__ATTRIBUTION_CANDIDATE_CAP__" "$ATTRIBUTION_CANDIDATE_CAP" \
        "__AGENT_ROOT__" "$AGENT_ROOT")"; then
        printf '%s\n' 'remote_agent: admission_render_failed: could not render the VM admission probe' >&2
        exit 1
    fi

    # 0) PRE-dispatch admission probe BEFORE any transfer cost. Exit 75 is the
    # same retryable-defer contract as the in-run check (TOCTOU belt-and-
    # suspenders — keep both).
    # shellcheck disable=SC2029
    _admission_probe_with_deadline <<REMOTE_EOF >&2
set -euo pipefail
mkdir -p "\$HOME/.local/lib/workbay"
exec 8>"\$HOME/.local/lib/workbay/.admission.lock"
flock -s -w 30 8 || { echo 'remote_agent: vm_admission_lock_timeout' >&2; exit 75; }
# A fresh consumer owns admission; do not reject jobs before they can queue.
if [ '${cmd}' = submit ]; then
    case '${AGENT_ROOT}' in /*) _queue_root='${AGENT_ROOT}' ;; *) _queue_root="\$HOME/${AGENT_ROOT}" ;; esac
    QUEUE_TICK_INTERVAL_S='${QUEUE_TICK_INTERVAL_S}'
    _queue_mtime="\$(stat -c %Y "\$_queue_root/.queue-state.json" 2>/dev/null || true)"
    case "\$_queue_mtime" in
        ''|*[!0-9]*) ;;
        *) case "\$QUEUE_TICK_INTERVAL_S" in
               ''|*[!0-9]*|0) ;;
               *) _queue_age=\$(( \$(date +%s) - _queue_mtime ))
                  _queue_freshness_limit_s=\$(( 2 * QUEUE_TICK_INTERVAL_S + 15 ))
                  if [ "\$_queue_age" -ge 0 ] && [ "\$_queue_age" -le "\$_queue_freshness_limit_s" ]; then exit 0; fi ;;
           esac ;;
    esac
fi
${_admission_remote_sh}
REMOTE_EOF

    # BEGIN DISPATCH_SOURCE_IDENTITY
    # AgentSpec was stamped before the first attempt and is the sole dispatch
    # authority. Do not re-read even refs/heads/$BRANCH here: the branch may move
    # between stamping, push, and an adapter re-dispatch. Exact requested_ref is
    # retained only as receipt metadata, never as an object selector.
    if [ "$AGENT_SPEC_BRANCH" != "$BRANCH" ]; then
        echo "remote_agent: agent-spec branch mismatch: expected $BRANCH, got $AGENT_SPEC_BRANCH" >&2
        exit 2
    fi
    case "$AGENT_SPEC_HEAD_SHA" in
        *[!0-9a-f]*|"") echo "remote_agent: invalid agent-spec head_sha" >&2; exit 2 ;;
    esac
    case "${#AGENT_SPEC_HEAD_SHA}" in 40|64) ;; *) echo "remote_agent: invalid agent-spec head_sha length" >&2; exit 2 ;; esac
    SOURCE_COMMIT="$AGENT_SPEC_HEAD_SHA"
    git cat-file -e "${SOURCE_COMMIT}^{commit}" 2>/dev/null \
        || { echo "remote_agent: agent-spec head_sha is not a local commit: ${SOURCE_COMMIT}" >&2; exit 2; }
    SOURCE_TREE="$(git rev-parse --verify "${SOURCE_COMMIT}^{tree}")" \
        || die "cannot resolve source tree: ${SOURCE_COMMIT}"
    # END DISPATCH_SOURCE_IDENTITY

    # 1) push the pinned commit to the remote clone (only committed state is built).
    # Push target is LANE_KEY + per-dispatch nonce: LANE_KEY alone is shared by
    # concurrent same-branch dispatches and would let the second overwrite the
    # first's ref mid-run. The local side of the refspec is the full immutable
    # commit id resolved above, never the branch name a concurrent writer can move.
    _phase "pushing $BRANCH -> ${REMOTE_HOST}:${REMOTE_DIR} (refs/heads/${LANE_KEY}-${DISPATCH_NONCE})"
    # BatchMode/ConnectTimeout (matching the SSH array): the push must FAIL FAST,
    # never prompt or hang, even if a caller leaves stdin attached — belt to the
    # `exec </dev/null` above (implementation note).
    # implementation note S1: transport = push only (absolute integer Unix seconds).
    _transport_start_ts="$(date +%s)"
    GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=4' \
        git push --quiet --force "${REMOTE_HOST}:${REMOTE_DIR}" "${SOURCE_COMMIT}:refs/heads/${LANE_KEY}-${DISPATCH_NONCE}" >&2
    _transport_end_ts="$(date +%s)"
    _emit_host_phase transport "$_transport_start_ts" "$_transport_end_ts"
    # Arm EXIT cleanup at the FIRST remote state (the push). Waiting until after
    # both scps leaves the pushed ref (and possibly the first copy) stranded on
    # a mid-staging failure [RES-13].
    _dispatch_staged=1

    # 2) ship brief + schema to the sandbox PARENT (survives the sandbox wipe).
    # Paths carry DISPATCH_NONCE so a concurrent same-branch dispatch cannot
    # overwrite this lane's inputs before/while the lock is held [CON-11].
    # implementation note S1: host_stage = mkdir + input scps (not the push).
    _host_stage_start_ts="$(date +%s)"
    "${SSH[@]}" "_agent_root='${AGENT_ROOT}'; case \"\$_agent_root\" in /*) ;; *) _agent_root=\"\$HOME/\$_agent_root\" ;; esac; mkdir -p \"\$_agent_root\"" >&2
    _scp_with_deadline "$BRIEF"  "${REMOTE_HOST}:${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md"   >&2
    _scp_with_deadline "$SCHEMA" "${REMOTE_HOST}:${AGENT_ROOT}/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json" >&2
    if [ -n "${AGENT_SPEC:-}" ]; then
        # JSON metadata (auth_match / result_source) + NUL argv sidecar.
        case "$AGENT_SPEC" in
            *.json) _agent_spec_json="$AGENT_SPEC" ;;
            *) _agent_spec_json="${AGENT_SPEC}.json" ;;
        esac
        [ -f "$_agent_spec_json" ] || die "--agent-spec json not found: ${_agent_spec_json}"
        _scp_with_deadline "$_agent_spec_json" \
            "${REMOTE_HOST}:${AGENT_ROOT}/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json" >&2
        _scp_with_deadline "$AGENT_SPEC_ARGV" \
            "${REMOTE_HOST}:${AGENT_ROOT}/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.argv" >&2
    fi
    _host_stage_end_ts="$(date +%s)"
    _emit_host_phase host_stage "$_host_stage_start_ts" "$_host_stage_end_ts"

    # Residual grok timeout after pre-dispatch probe + push + scp [RES-02].
    # --timeout is the caller's remote wall-clock budget (already under the local
    # transport bound minus post-grok fetch headroom). residual = max(0,
    # budget − elapsed); never floor it above the remaining budget (a 30s floor
    # could push remote past the local bound when pre-dispatch nears budget).
    # When residual hits 0, fail fast BEFORE starting grok rather than running
    # unbounded or overrunning the local transport bound.
    GROK_TIMEOUT=0
    if [ "$TIMEOUT" -gt 0 ]; then
        _elapsed=$(( $(date +%s) - BUILD_START_TS ))
        GROK_TIMEOUT=$(( TIMEOUT - _elapsed ))
        if [ "$GROK_TIMEOUT" -le 0 ]; then
            # Exit 75 (retryable defer), not 2: nothing is wrong with the
            # request — slow transport ate the budget pre-grok. A fresh
            # dispatch re-probes and retries; 2 would misread as caller error.
            echo "remote_agent: residual timeout exhausted after pre-dispatch" \
                 "(${_elapsed}s elapsed of ${TIMEOUT}s budget) — deferring lane" \
                 "before grok (remote must stay under the local transport bound)" >&2
            exit 75
        fi
    fi

    # 3) materialize hardened sandbox -> uv sync -> capped grok -> emit patch on stdout
    _emit_remote_body() {
        # Render the shared member reader host-side so the unquoted heredoc
        # injects it via an allowlisted host expansion, not a bare $(...) site.
        _LANE_SCOPE_CGROUP_MEMBERS_SRC="$(declare -f _lane_scope_cgroup_members)"
        printf '%s\n' "$_probe_trust_remote_sh"
        # Extractor needle (do not remove): "${SSH[@]}" env "WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB=$UV_CACHE_CAP_MB" bash -s <<REMOTE_EOF
        cat <<REMOTE_EOF
set -euo pipefail
export PATH="\$HOME/.grok/bin:\$PATH"
export GROK_ZDR_ENABLED=1
# implementation note: inject host-defined argv resolver (single source).
${_AGENT_SPEC_RESOLVER_SRC}
# implementation note S1: FIRST statements of the MAIN-BODY heredoc only — never the
# shared admission template (probe at pre-dispatch would fire first and
# swallow transport into ssh_connect). Unconditional; stderr only.
echo 'remote_agent: remote_body_start' >&2
_RP_ENTRY=\$(date +%s)
_PHASES_WARM_SKIP=0
_PHASES_PARTIAL=0
_PHASES_EMITTED=0
_PHASES_JSON_PARTS=''
_AGENT_START_TS=''
_AGENT_END_TS=''
_PORT_BACK_RECORDED=0
_AGENT_LAUNCH_OPEN_TS=''
# Record one VM phase as {start_ts,end_ts,duration_s} integer seconds [OBS-02].
_phase_record() {
    _ph_name="\$1"; _ph_s="\$2"; _ph_e="\$3"
    _ph_d=\$(( _ph_e - _ph_s ))
    _ph_frag=\$(printf '"%s":{"side":"vm","start_ts":%s,"end_ts":%s,"duration_s":%s}' "\$_ph_name" "\$_ph_s" "\$_ph_e" "\$_ph_d")
    _ph_status="\${4:-}"
    if [ -n "\$_ph_status" ]; then
        _ph_frag="\${_ph_frag%?},\"status\":\"\$_ph_status\"}"
    fi
    if [ -n "\$_PHASES_JSON_PARTS" ]; then
        _PHASES_JSON_PARTS="\${_PHASES_JSON_PARTS},\${_ph_frag}"
    else
        _PHASES_JSON_PARTS="\$_ph_frag"
    fi
}
# Fail-open write of OUT_DIR/.agent-phases.json (never stdout; never fatal).
_emit_phases_record() {
    # Guard only after a durable write succeeds [REV0192S1-A-03]. Setting it
    # before open("w") left zero-byte files unretriable by the EXIT trap.
    [ "\${_PHASES_EMITTED:-0}" = 1 ] && return 0
    [ -n "\${OUT_DIR:-}" ] || return 0
    [ -d "\$OUT_DIR" ] || return 0
    _RP_EXIT=\$(date +%s)
    if [ -n "\${_AGENT_END_TS:-}" ] && [ "\${_PORT_BACK_RECORDED:-0}" != 1 ]; then
        _phase_record port_back "\$_AGENT_END_TS" "\$_RP_EXIT"
        _PORT_BACK_RECORDED=1
    fi
    _vm_span=\$(( _RP_EXIT - _RP_ENTRY ))
    _vm_setup=\$_vm_span
    if [ "\${_PHASES_PARTIAL:-0}" != 1 ] && [ -n "\${_AGENT_START_TS:-}" ] && [ -n "\${_AGENT_END_TS:-}" ]; then
        _at=\$(( _AGENT_END_TS - _AGENT_START_TS ))
        _pb=\$(( _RP_EXIT - _AGENT_END_TS ))
        _vm_setup=\$(( _vm_span - _at - _pb ))
    fi
    _warm_json=false
    [ "\${_PHASES_WARM_SKIP:-0}" = 1 ] && _warm_json=true
    _partial_json=false
    [ "\${_PHASES_PARTIAL:-0}" = 1 ] && _partial_json=true
    _phases_path="\$OUT_DIR/.agent-phases.json"
    # One wide event. VM never emits setup / wall_seconds / completeness_class.
    if command -v python3 >/dev/null 2>&1; then
        if PHASES_JSON_PARTS="\$_PHASES_JSON_PARTS" \
        VM_SPAN="\$_vm_span" VM_SETUP="\$_vm_setup" \
        WARM_JSON="\$_warm_json" PARTIAL_JSON="\$_partial_json" \
        PHASES_PATH="\$_phases_path" \
        python3 - <<'PYPHASES'
import json, os, sys
parts = os.environ.get("PHASES_JSON_PARTS", "")
try:
    phases = json.loads("{" + parts + "}") if parts.strip() else {}
except json.JSONDecodeError:
    phases = {}
rec = {
    "schema_version": 1,
    "partial": os.environ.get("PARTIAL_JSON") == "true",
    "warm_skip": os.environ.get("WARM_JSON") == "true",
    "vm_span": int(os.environ.get("VM_SPAN", "0")),
    "vm_setup": int(os.environ.get("VM_SETUP", "0")),
    "phases": phases,
}
# Hard schema pin: host-owned keys must not appear on the VM half.
for banned in ("setup", "wall_seconds", "completeness_class", "host_span", "unaccounted"):
    rec.pop(banned, None)
path = os.environ["PHASES_PATH"]
tmp = path + ".tmp." + str(os.getpid())
try:
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, separators=(",", ":"))
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
except Exception:
    try:
        os.unlink(tmp)
    except OSError:
        pass
    sys.exit(1)
PYPHASES
        then
            _PHASES_EMITTED=1
        fi
    else
        # python3 absent: still emit a shell fallback so host fetch is not
        # byte-identical to a pre-materialize miss [REV0192S1-A-02].
        echo "remote_agent: phases writer: python3 unavailable — shell fallback" >&2
        _phases_tmp="\${_phases_path}.tmp.\$\$"
        if printf '{"schema_version":1,"partial":%s,"warm_skip":%s,"vm_span":%s,"vm_setup":%s,"phases":{}}\n' \
            "\$_partial_json" "\$_warm_json" "\$_vm_span" "\$_vm_setup" > "\$_phases_tmp" \
            && mv -f "\$_phases_tmp" "\$_phases_path"; then
            _PHASES_EMITTED=1
        else
            rm -f "\$_phases_tmp" 2>/dev/null || true
        fi
    fi
    return 0
}
# VM admission (RES-14 backpressure): re-check floor + lane cap at run start
# (TOCTOU vs pre-dispatch probe). Exit 75 → adapter maps to admission_deferred.
${_admission_remote_sh}
# RES-02 (S3): mark in-sandbox start so the setup + uv-sync time below can be
# subtracted from grok's residual budget. The pre-dispatch GROK_TIMEOUT math ran
# locally BEFORE this remote body, so it does not yet account for sandbox setup.
# _RP_ENTRY (above) is the vm_span / remote_preamble clock; _RP_START remains the
# residual-budget origin for grok (unchanged contract).
_RP_START=\$(date +%s)
SRC="\$HOME/${REMOTE_DIR}"
case '${AGENT_ROOT}' in /*) ROOT='${AGENT_ROOT}' ;; *) ROOT="\$HOME/${AGENT_ROOT}" ;; esac
SBX="\$ROOT/${LANE_KEY}"
# Persistent per-lane venv, kept OUTSIDE \$SBX so the per-pass 'rm -rf' below
# does not destroy it (internal). A warm re-dispatch
# then reconciles an already-populated env in seconds instead of rebuilding it
# from scratch — the fixed uv-sync overhead was eating the whole GROK_TIMEOUT
# on small slices. \$SBX is a deterministic per-lane path, so the venv's
# editable workspace-member links (into \$SBX/packages/*) stay valid across
# re-extracts; the venv holds only DERIVED deps (no repo history/secrets), so
# the history-stripped / remote-severed posture below is unaffected.
LANE_VENV="\$ROOT/.venv-lane-${LANE_KEY}"
# SBXVENV-01: uv cache for the SANDBOXED agent. Under codex
# '-s workspace-write -c sandbox_workspace_write.writable_roots=[".git"]' the
# whole of \$HOME is READABLE BUT READ-ONLY, so uv's default cache
# (\$HOME/.cache/uv) makes every in-sandbox 'uv sync' die with "Read-only file
# system (os error 30)" -- which 'make slice-start' reports as a degraded
# warning while STILL EXITING 0, laundering an environment fault into false RED
# evidence [OBS-08]. Probed on the VM 2026-09-03 under exactly those flags:
# /tmp IS writable, and it sits OUTSIDE the git workspace, so this cache can
# never be swept into 'git add -N .' or turn.patch. It also cannot grow by
# download -- the sandbox is remote-severed. Deliberately NOT exported at top
# level: the host-side 'uv sync' below has network and owns the real (13G)
# cache; redirecting THAT to /tmp would re-download it onto a 94%-full disk.
SBX_UV_CACHE="/tmp/workbay-uv-cache"
mkdir -p "\$SBX_UV_CACHE" 2>/dev/null || true
# BEGIN LANE_LOCK_GUARD
# SAME-BRANCH COLLISION GUARD (internal) [GRPH-09][CON-12].
# LANE_KEY is derived from the branch name ALONE, so two concurrent dispatches of
# the SAME branch resolve to one \$SBX. Without this lock the second lane's
# 'rm -rf "\$SBX"' below deletes the FIRST lane's LIVE working directory mid-run.
# Reproduced 2026-07-19: lane A died 'Unable to read current working directory'
# (exit 128) while lane B then failed to claim the held scope (exit 3) — both lost.
# NON-BLOCKING by design [RES-02][RES-03]: a blocking wait would stall for a full
# turn (~13 min), so defer fast on the EXISTING exit-75 retryable contract instead.
# Held for the life of this remote shell's critical section: same-key wipe and
# re-extract. The shell keeps fd 9 open; long-lived children close it (9>&-) so a
# backgrounded descendant cannot pin the lock after this shell exits (flock is on
# the open file description, not the process — "fd 9 closes on shell exit" alone
# is false if any child inherits it). The cross-lane LRU venv reap is NOT covered
# by this per-lane lock: it deletes OTHER keys' venvs and has no mutual exclusion
# against a concurrent different-lane reaper. The lockfile lives in \$ROOT so it
# survives the per-pass wipe. Holder PID is recorded best-effort for diagnosis.
# Mirrors the proven remote_gate.sh clone-lock pattern.
# NOTE: this makes same-branch dispatch SAFE, not PARALLEL. True same-branch
# concurrency needs per-dispatch sandbox keys and is deferred [REF-12][FM-05] —
# distinct branches already run concurrently up to the lane cap.
# Without flock, the dispatch cannot safely claim its own sandbox. Run only the
# bounded dispatch reaper first; its fallback mutex serializes cleanup, existing
# lane-lock files and leases still protect live workers, and the post-reaper gate
# below refuses materialization. This keeps cleanup useful without pretending an
# absent advisory-lock utility means a lane is busy [OBS-08][RES-07].
_lane_flock_available=1
if ! command -v flock >/dev/null 2>&1; then # exit 78 is deferred until after cleanup below
    _lane_flock_available=0
else
# Append open: truncating exec 9> blanks the lockfile at OPEN time, before
# flock, so a losing contender destroys the incumbent holder's pid diagnostic
# exactly when a contended lane is being investigated. flock locks the open
# file description regardless of open mode [OBS-05][CON-11].
# (No backticks in this comment: this body is an unquoted <<REMOTE_EOF heredoc;
# local command substitution would run at dispatch-construction time [AGT-10].)
exec 9>>"\$ROOT/.lane-lock-${LANE_KEY}"
flock -n 9 || { echo 'remote_agent: same-branch lane already active (${LANE_KEY}) — deferring' >&2; exit 75; }
# Best-effort holder identity for wedge diagnosis [OBS-05]; must never affect
# lock protocol or exit codes. Write by path after winning (safe under the
# held lock); fd-only append would accumulate stale holders under 9>>.
printf 'pid=%s\n' "\$\$" > "\$ROOT/.lane-lock-${LANE_KEY}" 2>/dev/null || true
fi
# END LANE_LOCK_GUARD
# Occupancy lease [RES-10][CON-11]: DECLARATION replaces host-variable inference
# (systemctl scope / fuser /proc). The occupant writes one file the script owns;
# observers read only that file. Binary outcome — no third "inconclusive" state:
#   absent | present+expired  → not occupied (return 1)
#   present+unexpired | malformed/unreadable → OCCUPIED (return 0; fail-safe)
# Path is under \$ROOT (NOT \$SBX) so the per-pass wipe cannot destroy the lease.
# One file per LANE_KEY: removed on EXIT, overwritten on the next dispatch of
# the same key — growth bounded by distinct branch count [RES-07].
# Expiry is derived ONCE from resolve_agent_bound's absolute deadline + margin
# (no refresher child — that child would itself be an orphan risk [CON-04]).
# Margin 300s: timeout -k grace, modest clock skew, setup/self-verify headroom.
# lease_expiry = max(_BOUND_DEADLINE, now) + 300 on every running arm [RES-02].
# Clock: absolute wall-clock expiry. Forward jump can expire a live lease early
# (next same-key dispatch may wipe) — no silent path; operator sees a fresh
# materialize. Backward jump: now < issued → still OCCUPIED (fail-safe); no
# refresher means the lease cannot extend itself indefinitely.
_lane_lease_file=
_lane_clear_live_lease() {
    [ -n "\${_lane_lease_file:-}" ] || return 0
    (
        exec 8>>"\$ROOT/.lane-lease-${LANE_KEY}.lock" || exit 0
        flock -x 8 2>/dev/null || exit 0
        [ -f "\$_lane_lease_file" ] || exit 0
        _lane_current_nonce=\$(sed -n 's/^nonce=//p' "\$_lane_lease_file" 2>/dev/null | head -n1)
        [ "\$_lane_current_nonce" = '${DISPATCH_NONCE}' ] || exit 0
        rm -f "\$_lane_lease_file" 2>/dev/null || true
    ) || true
}
_lane_occupant_live() {
    _lk="\${1:-}"
    # Empty key: fail-safe OCCUPIED (caller/reaper also short-circuits unparseable).
    [ -n "\$_lk" ] || return 0
    _lf="\$ROOT/.lane-live-\${_lk}"
    [ -f "\$_lf" ] || return 1
    _expiry=
    _issued=
    _released=
    while IFS= read -r _lline || [ -n "\$_lline" ]; do
        # Substring (not \${var#pfx}): extractors strip # as comments [TEST-04].
        case "\$_lline" in
            expiry=*) _expiry="\${_lline:7}" ;;
            issued=*) _issued="\${_lline:7}" ;;
            released=*) _released="\${_lline:9}" ;;
        esac
    done < "\$_lf" || return 0
    case "\$_expiry" in
        ''|*[!0-9]*) return 0 ;;
    esac
    case "\$_issued" in
        ''|*[!0-9]*) return 0 ;;
    esac
    case "\$_released" in
        1) return 1 ;;
        ''|0) ;;
        *) return 0 ;;
    esac
    _now=\$(date +%s)
    # Expired → clear (not occupied).
    if [ "\$_now" -ge "\$_expiry" ]; then
        return 1
    fi
    # Unexpired (including now < issued after a backward jump) → OCCUPIED.
    return 0
}
# BEGIN LANE_VENV_LRU_REAPER
# Native cache deletion is allowed only under the caller's global reap guard
# and candidate EX custody lock. Read-only probes never elevate privileges.
# A partial census is UNKNOWN, even when every readable process is idle.
_lane_cache_reap_locked() {
    _cache_guard_rc=0
    _cache_probe_seconds=15
    for _cache_deadline in "\${_BOUND_DEADLINE:-}" "\${_DISPATCH_SWEEP_DEADLINE:-}" "\${_LANE_CACHE_REAP_DEADLINE:-}"; do
        case "\$_cache_deadline" in ''|*[!0-9]*) continue ;; esac
        _cache_remaining=\$(( _cache_deadline - \$(date +%s) ))
        [ "\$_cache_remaining" -ge "\$_cache_probe_seconds" ] || _cache_probe_seconds=\$_cache_remaining
    done
    if [ "\$_cache_probe_seconds" -le 0 ]; then
        _lane_cache_hold "\$1" cache_probe_budget_exhausted "\$2" "\$3"
        return 1
    fi
    timeout -k 2 "\$_cache_probe_seconds" python3 - "\$ROOT" "\$1" "\$2" "\$3" "\$4" "\$5" '${LANE_KEY}' '${DISPATCH_NONCE}' "\${6:-cache}" <<'CACHEPY' || _cache_guard_rc=\$?
import ast
import fcntl
import json
import os
import re
import shutil
import stat
import sys
import time
from pathlib import Path

root, key, reason, actor, global_fd, key_fd, initiating_lane, nonce, scope = sys.argv[1:]
paired = scope == "pair"
cache = f".venv-lane-{key}"
stamp = f".venv-sync-stamp-{key}"
uid = os.getuid()
root_fd = None
delete_attempted = False
retained_fences = []
opened_fences = []

def journal(outcome, detail, **fields):
    row = dict(key=key, outcome=outcome, reason=reason, actor=actor,
               detail=detail, at=int(time.time()), **fields)
    # Only actual native scope, never a fabricated initiating job identity.
    if re.fullmatch(r"[A-Za-z0-9-]+", initiating_lane):
        row["initiating_lane"] = initiating_lane
    if re.fullmatch(r"[0-9]+-[0-9a-f]{16}", nonce):
        row["dispatch_nonce"] = nonce
    job_id = os.environ.get("JOB_ID", "")
    if re.fullmatch(r"j[0-9a-f]{20}", job_id):
        row["initiating_job_id"] = job_id
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK
    fd = os.open(".reap-outcomes.jsonl", flags, 0o600, dir_fd=root_fd)
    with os.fdopen(fd, "a") as stream:
        opened = os.fstat(stream.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_uid != uid:
            raise ValueError("journal_type_unknown")
        stream.write(json.dumps(row, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())

def hold(detail):
    journal("held", detail)
    raise SystemExit(1)

def identity(name, expected=None):
    try:
        s = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if s.st_uid != uid or stat.S_ISLNK(s.st_mode):
        raise ValueError("owner_or_symlink_unknown")
    if expected is not None and not expected(s.st_mode):
        raise ValueError("cache_type_unknown")
    return (s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_size, s.st_mtime_ns, s.st_ctime_ns)

def retain_fence(fd, names):
    # Numeric inherited descriptors are mandatory proof. Never reinterpret
    # EBADF (or a wrong inode/type/owner) as an absent candidate lock.
    s = os.fstat(fd)
    if not stat.S_ISREG(s.st_mode) or s.st_uid != uid:
        raise ValueError("custody_lock_type_or_owner_unknown")
    matches = []
    for name in names:
        current = identity(name, stat.S_ISREG)
        if current is not None and current[:2] == (s.st_dev, s.st_ino):
            matches.append(name)
    if not matches:
        raise ValueError("custody_lock_generation_changed")
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    current = identity(matches[0], stat.S_ISREG)
    if current is None or current[:2] != (s.st_dev, s.st_ino):
        raise ValueError("custody_lock_generation_changed")
    retained_fences.append((fd, matches[0]))
    return fd

def open_fence(name):
    before = identity(name, stat.S_ISREG)
    fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
                 0o600, dir_fd=root_fd)
    opened_fences.append(fd)
    if before is not None and before[:2] != (os.fstat(fd).st_dev, os.fstat(fd).st_ino):
        raise ValueError("custody_lock_generation_changed")
    return retain_fence(fd, (name,))

def validate_fences():
    for fd, name in retained_fences:
        s = os.fstat(fd)
        current = identity(name, stat.S_ISREG)
        if current is None or current[:2] != (s.st_dev, s.st_ino):
            raise ValueError("custody_lock_generation_changed")

def snapshot():
    # Source and native lease generation are rechecked with cache and stamp.
    names = (cache, stamp, key, f".lane-live-{key}", f".lane-lock-{key}",
             f".lane-lease-{key}.lock", ".reap.lock", ".reap-sweep.lock",
             f"{key}/.git", f"{key}/pyproject.toml", f"{key}/uv.lock",
             f"{key}/.workbay-lane-sandbox")
    result = {name: identity(name) for name in names}
    identity(cache, stat.S_ISDIR)
    identity(stamp, stat.S_ISREG)
    entries = {}
    inodes = set()
    for tree in (cache, key) if paired else (cache,):
      if result[tree] is not None:
        for parent, dirs, files in os.walk(Path(root) / tree, followlinks=False,
                                           onerror=lambda e: (_ for _ in ()).throw(e)):
            for name in [Path(parent), *(Path(parent) / n for n in dirs + files)]:
                s = name.lstat()
                if s.st_uid != uid or s.st_dev != result[tree][0]:
                    raise ValueError("foreign_cache_entry")
                if not (stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode) or stat.S_ISLNK(s.st_mode)):
                    raise ValueError("cache_entry_type_unknown")
                # Shared hardlink storage cannot be attributed to this cache.
                if stat.S_ISREG(s.st_mode) and s.st_nlink != 1:
                    raise ValueError("shared_cache_entry")
                entries[str(name)] = (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                inodes.add((s.st_dev, s.st_ino))
    result["entries"] = entries
    return result, inodes

def lease_idle():
    name = f".lane-live-{key}"
    if identity(name) is None:
        return True
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root_fd)
    with os.fdopen(fd) as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("lease_type_unknown")
        fields = dict(line.rstrip("\n").split("=", 1) for line in stream)
    issued, expiry = int(fields["issued"]), int(fields["expiry"])
    released = fields.get("released", "0")
    if released not in {"0", "1"} or issued < 0 or expiry < issued:
        raise ValueError("lease_unknown")
    # Even a released/expired declaration cannot override a live PID.
    pid = fields.get("pid", "")
    if not pid.isdigit():
        raise ValueError("lease_pid_unknown")
    if Path(f"/proc/{pid}").exists():
        return False
    return released == "1" or time.time() >= expiry

def references(inodes):
    targets = [str(Path(root) / n) for n in (cache, stamp, key)]
    def matches(value):
        value = value.removesuffix(" (deleted)")
        return any(value == t or value.startswith(t + "/") for t in targets)
    def inventory(directory, *, native=False):
        # Incoming dependency metadata remains meaningful after all import
        # files close. Never execute .pth/finder code to inspect it. Unsupported
        # executable/dynamic metadata is UNKNOWN, not a negative reference.
        count = 0
        for parent, dirs, files in os.walk(directory, followlinks=False,
                                         onerror=lambda e: (_ for _ in ()).throw(e)):
            if native and Path(parent) == Path(root):
                dirs[:] = [n for n in dirs if n != cache and not (paired and n == key)]
            for name in dirs + files:
                count += 1
                if count > 50000:
                    raise ValueError("incoming_inventory_partial")
                path = Path(parent) / name
                s = path.lstat()
                if stat.S_ISLNK(s.st_mode):
                    # Strict resolution detects broken/looping metadata too.
                    resolved = path.resolve(strict=True)
                    if matches(str(resolved)):
                        return "warm_cache_alias" if name == ".venv" else "incoming_package_link"
                    if name.endswith((".pth", ".egg-link")) or (resolved.is_dir() and
                            resolved != Path(root) and Path(root) not in resolved.parents):
                        raise ValueError("incoming_link_domain_unfenced")
                elif name.endswith((".pth", ".egg-link")) or (name.startswith("__editable__") and name.endswith(".py")):
                    if not stat.S_ISREG(s.st_mode) or s.st_size > 2 * 1024 * 1024:
                        raise ValueError("incoming_metadata_partial")
                    text = path.read_text()
                    if name.endswith(".py"):
                        tree = ast.parse(text)
                        # Static string constants cover setuptools MAPPING and
                        # NAMESPACES; any executable finder remains unverifiable.
                        for node in ast.walk(tree):
                            if isinstance(node, ast.Constant) and isinstance(node.value, str) and matches(node.value):
                                return "incoming_editable_finder"
                        raise ValueError("dynamic_editable_metadata_unknown")
                    for line in text.splitlines():
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith(("import ", "import\t")):
                            raise ValueError("dynamic_pth_metadata_unknown")
                        target = Path(line)
                        if not target.is_absolute():
                            target = path.parent / target
                        if matches(str(target.resolve())):
                            return "incoming_pth_reference"
                    # External metadata has no native reader/writer fence.
                    # Even disjoint metadata could acquire a new target later.
                    if not native and s.st_uid == uid:
                        raise ValueError("external_metadata_unfenced")
        return None

    incoming = inventory(Path(root), native=True)
    if incoming:
        return incoming
    # A mount inside the tree is outside this directory's deletion authority.
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        mount = line.split()[4]
        if chr(92) in mount:
            raise ValueError("mount_probe_unknown")
        if matches(mount):
            return "cache_mount"
    def generation(pid):
        raw = (Path("/proc") / pid / "stat").read_text()
        parts = raw.rsplit(")", 1)[1].split()
        return parts[19], parts[0]  # starttime and state, not PID alone
    pids = sorted(p.name for p in Path("/proc").iterdir() if p.name.isdigit())
    generations = {}
    unknown = False
    unknown_detail = "proc_census_unknown"
    live = None
    for pid in pids:
        proc = Path("/proc") / pid
        try:
            before = generation(pid)
            generations[pid] = before
            if before[1] in {"Z", "X"}:
                if generation(pid) != before:
                    unknown = True
                continue
            for component in ("cwd", "exe", "root"):
                if matches(os.readlink(proc / component)):
                    live = "live_reference"
            # FD aliases are checked by both target path and inode.
            for fd in (proc / "fd").iterdir():
                try:
                    if matches(os.readlink(fd)):
                        live = "live_fd"
                    s = fd.stat()
                except FileNotFoundError:
                    # Listing our own fd directory includes the temporary
                    # enumeration fd, closed by listdir before its return.
                    # No other process receives this exemption.
                    if int(pid) == os.getpid():
                        continue
                    raise
                if (s.st_dev, s.st_ino) in inodes:
                    live = "live_fd"
            env = (proc / "environ").read_bytes().decode(errors="surrogateescape")
            maps = (proc / "maps").read_text(errors="surrogateescape")
            if any(t in env for t in targets):
                live = "live_environment"
            if any(t in maps for t in targets):
                live = "live_maps"
            # Bounded supported external environments: declared virtualenv,
            # Python path, interpreter prefix/site directories, and venv argv.
            # A denied/partial/dynamic domain forces HOLD. We do not infer
            # mutable interpreter sys.path from closed FDs.
            fields = dict(item.split("=", 1) for item in env.split(chr(0)) if "=" in item)
            domains = []
            if fields.get("VIRTUAL_ENV"):
                domains.append(Path(fields["VIRTUAL_ENV"]))
            for value in fields.get("PYTHONPATH", "").split(os.pathsep):
                if value:
                    domains.append(Path(value))
            executable = Path(os.readlink(proc / "exe"))
            if executable.name.startswith("python"):
                if int(pid) != os.getpid():
                    # Closed import files cannot attest an interpreter's
                    # in-memory sys.path/meta_path. An own .venv link is
                    # harmless only without a possible unfenced live reader.
                    live = "live_interpreter_metadata_unknown"
                prefix = executable.parent.parent
                domains.extend((prefix / "lib").glob("python*/site-packages"))
                # argv preserves the venv prefix when exe resolves a symlink.
                argv = (proc / "cmdline").read_bytes().split(bytes([0]))
                if argv and argv[0].startswith(b"/"):
                    command = Path(os.fsdecode(argv[0]))
                    if (command.parent.parent / "pyvenv.cfg").exists():
                        domains.append(command.parent.parent)
            for domain in domains:
                if not domain.is_absolute():
                    raise ValueError("external_environment_root_unknown")
                domain = domain.resolve(strict=True)
                if domain == Path(root) or Path(root) in domain.parents:
                    continue  # already inventoried under native fences
                detail = inventory(domain)
                if detail:
                    live = detail
            if generation(pid) != before:
                unknown = True
        except (OSError, ValueError, IndexError) as exc:
            # A vanished/changing PID is not a complete negative observation.
            unknown = True
            if isinstance(exc, ValueError):
                unknown_detail = str(exc)
    if live:
        return live
    if sorted(p.name for p in Path("/proc").iterdir() if p.name.isdigit()) != pids:
        raise ValueError("pid_generation_changed")
    for pid, before in generations.items():
        if generation(pid) != before:
            raise ValueError("pid_generation_changed")
    if unknown:
        raise ValueError(unknown_detail)
    return None

try:
    if not re.fullmatch(r"[A-Za-z0-9-]+-[0-9a-f]{8}", key):
        raise ValueError("cache_key_unknown")
    if str(Path(root).resolve(strict=True)) != root:
        raise ValueError("root_alias_unknown")
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    if os.fstat(root_fd).st_uid != uid:
        hold("root_owner_unknown")
    # Shell flock may be unavailable, or the read-only candidate probe may
    # have found no key inode. These explicit states alone authorize native
    # creation. Acquire the canonical global fence BEFORE any candidate fence;
    # the mkdir sweep mutex does not fence archival/native writers.
    if global_fd == "native":
        if key_fd not in {"absent", "native"}:
            raise ValueError("custody_global_before_key_required")
        open_fence(".reap-sweep.lock")
    else:
        for fd_text in global_fd.split(","):
            retain_fence(int(fd_text), (".reap.lock", ".reap-sweep.lock"))
    if key_fd in {"absent", "native"}:
        open_fence(f".lane-lock-{key}")
    else:
        retain_fence(int(key_fd), (f".lane-lock-{key}",))
    # The installed consumer already takes this publication fence around job
    # acquisition/launch. Retain it so a new native reader cannot appear between
    # inventory and unlink; existing readers are fenced by their native keys.
    publication_fd = open_fence(".queue-publication.lock")
    lease_name = f".lane-lease-{key}.lock"
    lease_fd = open_fence(lease_name)
    # Fence native incoming-reference writers, not merely two observations of
    # them. Hold every existing other lane's key lock through the actual delete.
    # An external metadata domain without these fences is always HOLD above.
    incoming_fds = []
    incoming_keys = set()
    for entry in Path(root).iterdir():
        name = entry.name
        other = name
        for prefix in (".venv-lane-", ".lane-lock-"):
            if name.startswith(prefix):
                other = name.removeprefix(prefix)
        if other in {key, initiating_lane} or other in incoming_keys or not re.fullmatch(r"[A-Za-z0-9-]+-[0-9a-f]{8}", other):
            continue
        incoming_keys.add(other)
        lock = f".lane-lock-{other}"
        fd = open_fence(lock)
        incoming_fds.append(fd)
    validate_fences()
    before, inodes = snapshot()
    if before[cache] is None and before[stamp] is None and not paired:
        raise SystemExit(0)
    if not lease_idle():
        hold("live_native_lease")
    detail = references(inodes)
    if detail:
        hold(detail)
    if snapshot()[0] != before or not lease_idle():
        hold("cache_generation_changed")
    age = max(0, int(time.time() - before[cache][5] / 1e9)) if before[cache] else 0
    size = sum(item[2] for item in before["entries"].values())
    if before[stamp]:
        size += before[stamp][4]
    journal("started", "complete_unreferenced_census", age=age, bytes_before=size)
    # Last fresh checks are under the same retained global/key/lease fences.
    # Uncooperative external writers are not made safe by advisory locks.
    if references(inodes) or snapshot()[0] != before or not lease_idle():
        hold("late_reference_or_generation_change")
    validate_fences()
    if before[cache] is not None:
        fd = os.open(cache, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
        s = os.fstat(fd)
        if (s.st_dev, s.st_ino) != before[cache][:2] or identity(cache) != before[cache]:
            hold("cache_generation_changed")
        os.close(fd)
        delete_attempted = True
        shutil.rmtree(cache, dir_fd=root_fd)
    if before[stamp] is not None:
        if identity(stamp) != before[stamp]:
            raise ValueError("stamp_generation_changed")
        delete_attempted = True
        os.unlink(stamp, dir_fd=root_fd)
    if paired and before[key] is not None:
        # Source/cache/stamp are one guarded custody decision. Inherited
        # global/key plus lease and incoming-domain locks stay open until here.
        if identity(key) != before[key]:
            raise ValueError("source_generation_changed")
        delete_attempted = True
        shutil.rmtree(key, dir_fd=root_fd)
    journal("reaped", "complete_unreferenced_census", age=age, bytes_freed=size)
except SystemExit:
    raise
except (OSError, ValueError, KeyError, IndexError, SyntaxError, UnicodeError, RuntimeError) as exc:
    if root_fd is not None:
        try:
            journal("incomplete" if delete_attempted else "held",
                    str(exc) if isinstance(exc, ValueError) else "probe_or_delete_unknown",
                    **(dict(age=age, step="delete", status=1) if delete_attempted else {}))
        except OSError:
            pass
    disposition = "cache deletion incomplete" if delete_attempted else "cache custody HOLD"
    print(f"remote_agent: {disposition} for {key}: {exc}", file=sys.stderr)
    raise SystemExit(1)
finally:
    # Closing a child copy preserves a caller's inherited lock. Native-only
    # fences are released on success, HOLD, probe failure, and delete failure.
    for fd in set(opened_fences + [fd for fd, _ in retained_fences]):
        os.close(fd)
    if root_fd is not None:
        os.close(root_fd)
CACHEPY
    case "\$_cache_guard_rc" in
        0) return 0 ;;
        1) return 1 ;;
        *)
            _lane_cache_hold "\$1" cache_probe_timeout_or_unavailable "\$2" "\$3"
            return 1 ;;
    esac
}
_lane_cache_hold() {
    # No-follow journal open also applies to lock/probe failures before the
    # guarded Python body runs. A timed-out deletion is never called a HOLD.
    timeout -k 1 2 python3 - "\$ROOT" "\$1" "\$2" "\${3:-lru_venv}" "\${4:-dispatch_lru}" <<'HOLDPY' || \
        echo "remote_agent: cannot journal cache HOLD for \$1" >&2
import json
import os
import stat
import sys
import time
root, key, detail, reason, actor = sys.argv[1:]
root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
if os.fstat(root_fd).st_uid != os.getuid():
    raise ValueError("journal root owner unknown")
fd = os.open(".reap-outcomes.jsonl", os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
             0o600, dir_fd=root_fd)
with os.fdopen(fd, "a+") as stream:
    opened = os.fstat(stream.fileno())
    if not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid():
        raise ValueError("journal type or owner unknown")
    outcome = "held"
    if detail == "cache_probe_timeout_or_unavailable":
        stream.seek(0)
        last = None
        for line in stream:
            row = json.loads(line)
            if row.get("key") == key and row.get("reason") == reason and row.get("actor") == actor:
                last = row.get("outcome")
        if last == "started":
            outcome = "incomplete"
    stream.write(json.dumps(dict(key=key, outcome=outcome, reason=reason,
                                detail=detail, actor=actor, at=int(time.time()))) + "\n")
    stream.flush()
    os.fsync(stream.fileno())
HOLDPY
}
_lane_lru_reap_candidate() {
    _old="\${1:-}"
    _oldkey="\${_old##*/.venv-lane-}"
    case "\$_oldkey" in ''|*[!A-Za-z0-9-]*) return 0 ;; esac
    [ "\$_old" = "\$ROOT/.venv-lane-\$_oldkey" ] || return 0
    (
        # Match global->candidate order used by native reapers. Nonblocking
        # custody conflicts always retain, including archival key-lock holders.
        for _cache_lock in "\$ROOT/.reap.lock" "\$ROOT/.reap-sweep.lock" "\$ROOT/.lane-lock-\$_oldkey"; do
            if [ -L "\$_cache_lock" ] || { [ -e "\$_cache_lock" ] && [ ! -f "\$_cache_lock" ]; }; then
                _lane_cache_hold "\$_oldkey" custody_lock_type_unknown
                exit 0
            fi
        done
        exec 5>>"\$ROOT/.reap.lock" || exit 0
        flock -n 5 || { _lane_cache_hold "\$_oldkey" global_reap_lock_held; exit 0; }
        exec 6>>"\$ROOT/.reap-sweep.lock" || exit 0
        flock -n 6 || { _lane_cache_hold "\$_oldkey" global_sweep_lock_held; exit 0; }
        exec 8>>"\$ROOT/.lane-lock-\${_oldkey}" || exit 0
        flock -n 8 || { _lane_cache_hold "\$_oldkey" native_custody_lock_held; exit 0; }
        if _lane_occupant_live "\$_oldkey"; then
            _lane_cache_hold "\$_oldkey" live_native_lease
            exit 0
        fi
        _cache_rc=0
        _lane_cache_reap_locked "\$_oldkey" lru_venv dispatch_lru 5,6 8 || _cache_rc=\$?
        case "\$_cache_rc" in
            0|1) ;;
            *) _lane_cache_hold "\$_oldkey" cache_probe_timeout_or_unavailable ;;
        esac
    )
}
if [ '${LANE_VENV_CAP}' -gt 0 ] 2>/dev/null; then
    _LANE_CACHE_REAP_DEADLINE=\$(( \$(date +%s) + 15 ))
    ls -1dt "\$ROOT"/.venv-lane-* 2>/dev/null | tail -n +\$(( ${LANE_VENV_CAP} + 1 )) | while IFS= read -r _old; do
        [ "\$_old" = "\$LANE_VENV" ] && continue
        _lane_lru_reap_candidate "\$_old"
    done || true
fi
# END LANE_VENV_LRU_REAPER
_lane_write_live_lease() {
    _lane_lease_file="\$ROOT/.lane-live-${LANE_KEY}"
    _now=\$(date +%s)
    # Single derivation: bound ladder wrote expiry once [RES-02][TEST-15].
    # Hard precondition — no default: a fallback expiry would silently diverge
    # from the process bound. Diagnose before assign so the read stays a bare
    # expansion (mutation targets still match) [AGT-10].
    : "\${_BOUND_LEASE_EXPIRY:?remote_agent: bound ladder did not run before lease write}"
    _expiry=\$_BOUND_LEASE_EXPIRY
    # return (not exit): keeps producer→adapter exit-N completeness closed without
    # a new hard-fail arm; set -e on the bare call still aborts before wipe.
    if ! (
        exec 8>>"\$ROOT/.lane-lease-${LANE_KEY}.lock" || { echo 'remote_agent: lane lease unavailable (lane_lease_unavailable)' >&2; exit 1; }
        flock -x 8 2>/dev/null || { echo 'remote_agent: lane lease unavailable (lane_lease_unavailable)' >&2; exit 1; }
        printf 'pid=%s\nissued=%s\nexpiry=%s\nnonce=%s\n' \
            "\$\$" "\$_now" "\$_expiry" '${DISPATCH_NONCE}' > "\${_lane_lease_file}.tmp" \
            && mv -f "\${_lane_lease_file}.tmp" "\$_lane_lease_file"
    ); then
        echo 'remote_agent: failed to write occupancy lease — refusing to wipe sandbox' >&2
        return 1
    fi
    trap '_lane_clear_live_lease' EXIT
}
# Preserve a warm sandbox's working-tree bytes before the same-branch retry
# wipes it. The snapshot is outside \$SBX and uses the same intent-to-add +
# binary-diff union as post-agent harvest, so tracked edits and untracked files
# remain inspectable even when the prior dispatch never reached its salvage arm.
# A capture failure refuses the destructive wipe; a clean/absent marked sandbox
# is a no-op. Git config and hook execution are isolated because this runs before
# the post-agent harvest sanitizer exists [RES-02][RES-06][OBS-08].
_snapshot_dirty_sandbox() {
    [ -d "\$SBX" ] || return 0
    [ -f "\$SBX/.workbay-lane-sandbox" ] || return 0
    [ -d "\$ROOT" ] || return 1
    _snapshot_path=\$(mktemp "\$ROOT/.salvage-${LANE_KEY}-XXXXXX.patch") || return 1
    _snapshot_git() {
        _snapshot_now=\$(date +%s) || return 124
        case "\${_BOUND_DEADLINE:-}" in
            ''|*[!0-9]*) _snapshot_remaining=30 ;;
            *) _snapshot_remaining=\$(( _BOUND_DEADLINE - _snapshot_now )) ;;
        esac
        [ "\$_snapshot_remaining" -gt 0 ] || return 124
        if command -v timeout >/dev/null 2>&1; then
            timeout -k 1 "\$_snapshot_remaining" env -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT \
                GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null \
                GIT_CONFIG_NOSYSTEM=1 GIT_NO_LAZY_FETCH=1 GIT_NO_REPLACE_OBJECTS=1 \
                git -c protocol.file.allow=never -c protocol.ext.allow=never \
                -c protocol.ssh.allow=never -c protocol.http.allow=never \
                -c protocol.https.allow=never -c protocol.git.allow=never \
                -c core.hooksPath=/dev/null -c core.fsmonitor=false "\$@"
        elif [ "\${_SCOPE_SUPPORTS_RUNTIMEMAX:-0}" -eq 1 ]; then
            systemd-run --quiet --user --scope -p RuntimeMaxSec="\$_snapshot_remaining" \
                env -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT \
                GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null \
                GIT_CONFIG_NOSYSTEM=1 GIT_NO_LAZY_FETCH=1 GIT_NO_REPLACE_OBJECTS=1 \
                git -c protocol.file.allow=never -c protocol.ext.allow=never \
                -c protocol.ssh.allow=never -c protocol.http.allow=never \
                -c protocol.https.allow=never -c protocol.git.allow=never \
                -c core.hooksPath=/dev/null -c core.fsmonitor=false "\$@"
        else
            return 124
        fi
    }
    if ! (
        cd "\$SBX" || exit 1
        # SALVAGE-SNAPSHOT-ADDN-004: an intent-to-add failure leaves an
        # untracked-only tree invisible to diff HEAD; never turn that into an
        # empty snapshot before the destructive warm-sandbox wipe [OBS-08].
        if ! _snapshot_git add -N . >/dev/null 2>&1; then
            exit 1
        fi
        _snapshot_git diff --binary --no-textconv --no-ext-diff HEAD > "\$_snapshot_path" 2>/dev/null
    ); then
        rm -f "\$_snapshot_path" 2>/dev/null || true
        echo 'remote_agent: snapshot_refusal=warm_dirty_sandbox refusing to wipe existing sandbox' >&2
        return 1
    fi
    if [ ! -s "\$_snapshot_path" ]; then
        rm -f "\$_snapshot_path" 2>/dev/null || true
        return 0
    fi
    echo "remote_agent: warm sandbox dirty snapshot captured to \$_snapshot_path" >&2
    return 0
}
# Bound ladder (implementation note): resolve once before admission; lease + TW + RUNNER
# all read the same deadline. Arms: wrapper | scope | ceiling | refuse-exit-7.
# Whole-string helpers keep assembly under extractable functions [TEST-15].
resolve_agent_bound() {
    # Two independent capability probes — never combined (a host that supports
    # MemoryMax but not RuntimeMaxSec must keep its cgroup on arm 1).
    _SCOPE_OK=0
    _SCOPE_SUPPORTS_RUNTIMEMAX=0
    if systemd-run --quiet --user --scope -p MemoryMax=${MEM_MAX} true 2>/dev/null; then
        _SCOPE_OK=1
    fi
    if systemd-run --quiet --user --scope -p RuntimeMaxSec=60 true 2>/dev/null; then
        _SCOPE_SUPPORTS_RUNTIMEMAX=1
    fi
    _has_timeout=0
    if command -v timeout >/dev/null 2>&1; then
        _has_timeout=1
    fi
    if [ '${GROK_TIMEOUT}' -gt 0 ] 2>/dev/null; then
        _BOUND_DEADLINE=\$(( _RP_START + ${GROK_TIMEOUT} ))
        if [ "\$_has_timeout" -eq 1 ]; then
            _BOUND_MODE=wrapper
        elif [ "\$_SCOPE_SUPPORTS_RUNTIMEMAX" = 1 ]; then
            _BOUND_MODE=scope
        else
            # Arm 4: no bound obtainable — refuse before lease/wipe [AGT-10].
            echo "remote_agent: no process bound available — timeout(1) absent and RuntimeMaxSec unsupported — refusing unbounded dispatch" >&2
            exit 7
        fi
    else
        _BOUND_DEADLINE=\$(( _RP_START + ${WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S} ))
        if [ "\$_SCOPE_SUPPORTS_RUNTIMEMAX" = 1 ]; then
            _BOUND_MODE=ceiling
        elif [ "\$_has_timeout" -eq 1 ]; then
            # timeout=0 without RuntimeMaxSec: still bound via timeout(1) ceiling.
            _BOUND_MODE=wrapper
        else
            # Arm 4: neither control present [AGT-10].
            echo "remote_agent: no process bound available — timeout(1) absent and RuntimeMaxSec unsupported — refusing unbounded dispatch" >&2
            exit 7
        fi
    fi
    _now=\$(date +%s)
    _margin=300
    # Floor: max(deadline, now) + margin (preserved when residual already gone).
    if [ "\$_BOUND_DEADLINE" -gt "\$_now" ]; then
        _BOUND_LEASE_EXPIRY=\$(( \$_BOUND_DEADLINE + _margin ))
    else
        _BOUND_LEASE_EXPIRY=\$(( _now + _margin ))
    fi
}
_agent_bound_runner() {
    # Entire RUNNER string — never a RuntimeMaxSec fragment alone [TEST-15].
    # _SCOPE_OK and _SCOPE_SUPPORTS_RUNTIMEMAX are independent probes: MemoryMax
    # needs cgroup memory delegation; RuntimeMaxSec is a pure timer. When the
    # host supports RuntimeMaxSec but not MemoryMax, emit RuntimeMaxSec only so
    # scope/ceiling arms stay bounded instead of falling through to bare nice
    # (which would silently drop the bound when TW is empty) [TEST-15].
    # Budget is a positional from the caller (same shape as _agent_bound_wrapper_prefix).
    # Never re-read the clock here — a fallback would silence a broken thread.
    _n="\${1:-}"
    _agent_unit="\${2:-\${LANE_UNIT}}"
    if [ -z "\$_n" ]; then
        echo "remote_agent: bound runner requires a residual budget — refusing empty RuntimeMaxSec (unbounded)" >&2
        return 1
    fi
    if [ "\$_SCOPE_OK" = 1 ]; then
        if [ "\$_BOUND_MODE" = scope ] || [ "\$_BOUND_MODE" = ceiling ]; then
            printf '%s\n' "systemd-run --quiet --user --scope --unit \$_agent_unit -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA} -p RuntimeMaxSec=\$_n nice -n 10 ionice -c3"
        else
            printf '%s\n' "systemd-run --quiet --user --scope --unit \$_agent_unit -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA} nice -n 10 ionice -c3"
        fi
    elif [ "\$_BOUND_MODE" = scope ] || [ "\$_BOUND_MODE" = ceiling ]; then
        # MemoryMax unsupported; RuntimeMaxSec available — keep the process
        # bound, drop MemoryMax/CPUQuota (this host rejects them).
        printf '%s\n' "systemd-run --quiet --user --scope --unit \$_agent_unit -p RuntimeMaxSec=\$_n nice -n 10 ionice -c3"
    else
        printf '%s\n' 'nice -n 10 ionice -c3'
    fi
}
_agent_bound_wrapper_prefix() {
    # Entire TW string per mode — residual check stays in the parent shell.
    _n="\${1:-}"
    if [ "\$_BOUND_MODE" = wrapper ]; then
        printf '%s\n' "timeout -k 10 \$_n"
    else
        printf '%s\n' ''
    fi
}
# Per-dispatch transient reaper [RES-07]: age-based TTL for nonce'd outbox dirs,
# brief files, schema files, and dispatch refs (loose + packed). Count caps are
# wrong-shaped for per-dispatch growth. Age is the live-dispatch guard — never
# delete younger than TTL. Best-effort / fail-open (must never wedge the pass).
# 0 = disable. EXIT trap remains the fast path for THIS dispatch; this is the
# backstop for leaks when the trap does not run. Own DISPATCH_NONCE is excluded
# from every sweep so a short operator TTL cannot delete this turn's inputs.
# Ref age comes from the reflog (survives pack-refs); packed refs are enumerated
# via for-each-ref (loose walk is a no-op after receive-pack gc --auto).
if [ '${DISPATCH_TTL_SEC}' -gt 0 ] 2>/dev/null; then
    {
        _dispatch_reaper_guard_path=
        _dispatch_reaper_guard_held=0
        _dispatch_reaper_mutex_available=1
        _dispatch_reaper_guard_release() {
            [ "\${_dispatch_reaper_guard_held:-0}" -eq 1 ] || return 0
            rm -f "\$_dispatch_reaper_guard_path/owner" 2>/dev/null || true
            if ! rmdir "\$_dispatch_reaper_guard_path" 2>/dev/null; then
                echo 'remote_agent: dispatch reaper mutex release failed' >&2
                return 1
            fi
            _dispatch_reaper_guard_held=0
        }
        _dispatch_reaper_guard_owner_live() {
            _drs_pid="\${1:-}"
            case "\$_drs_pid" in ''|0|*[!0-9]*) return 0 ;; esac
            kill -0 "\$_drs_pid" 2>/dev/null && return 0
            if [ -d /proc ]; then
                [ -d "/proc/\$_drs_pid" ]
                return
            fi
            command -v ps >/dev/null 2>&1 || return 0
            _drs_ps_rc=0
            _drs_ps_output=\$(ps -axo pid= 2>/dev/null) || _drs_ps_rc=\$?
            [ "\$_drs_ps_rc" -eq 0 ] || return 0
            while IFS= read -r _drs_ps_line || [ -n "\$_drs_ps_line" ]; do
                if [[ "\$_drs_ps_line" =~ ^[[:space:]]*$ ]]; then
                    continue
                fi
                if [[ "\$_drs_ps_line" =~ ^[[:space:]]*([0-9]+)[[:space:]]*$ ]]; then
                    [ "\${BASH_REMATCH[1]}" = "\$_drs_pid" ] && return 0
                else
                    return 0
                fi
            done <<< "\$_drs_ps_output"
            return 1
        }
        _dispatch_reaper_guard_recover_stale() {
            _drs_path="\${1:-}"
            [ -n "\$_drs_path" ] && [ -d "\$_drs_path" ] && [ ! -L "\$_drs_path" ] || return 1
            [ -f "\$_drs_path/owner" ] && [ ! -L "\$_drs_path/owner" ] || return 1
            _drs_owner=\$(cat "\$_drs_path/owner" 2>/dev/null) || return 1
            case "\$_drs_owner" in
                pid=[1-9][0-9]*) _drs_pid="\${_drs_owner#pid=}" ;;
                *) return 1 ;;
            esac
            case "\$_drs_pid" in ''|*[!0-9]*) return 1 ;; esac
            _dispatch_reaper_guard_owner_live "\$_drs_pid" && return 1
            _drs_recovery="\${_drs_path}.stale.\$\$"
            [ ! -e "\$_drs_recovery" ] && [ ! -L "\$_drs_recovery" ] || return 1
            mv "\$_drs_path" "\$_drs_recovery" 2>/dev/null || return 1
            _drs_moved_owner=\$(cat "\$_drs_recovery/owner" 2>/dev/null) || _drs_moved_owner=
            if [ "\$_drs_moved_owner" != "\$_drs_owner" ] || _dispatch_reaper_guard_owner_live "\$_drs_pid"; then
                if [ ! -e "\$_drs_path" ] && [ ! -L "\$_drs_path" ]; then
                    mv "\$_drs_recovery" "\$_drs_path" 2>/dev/null || true
                fi
                return 1
            fi
            rm -f "\$_drs_recovery/owner" 2>/dev/null || return 1
            rmdir "\$_drs_recovery" 2>/dev/null || return 1
            return 0
        }
        if [ "\${_lane_flock_available:-1}" -eq 0 ]; then
            # mkdir is an atomic mutex on ROOT. In sweep-only mode, all
            # reapers serialize here; no new worker may start before the
            # post-reaper configuration error below.
            _dispatch_reaper_guard_path="\$ROOT/.dispatch-reaper.guard"
            if mkdir "\$_dispatch_reaper_guard_path" 2>/dev/null; then
                _dispatch_reaper_guard_held=1
                if ! printf 'pid=%s\n' "\$\$" >"\$_dispatch_reaper_guard_path/owner"; then
                    _dispatch_reaper_guard_release || true
                    _dispatch_reaper_mutex_available=0
                    echo 'remote_agent: dispatch reaper mutex owner record failed' >&2
                else
                    trap '_dispatch_reaper_guard_release' EXIT
                fi
            else
                _guard_recovered=0
                if _dispatch_reaper_guard_recover_stale "\$_dispatch_reaper_guard_path" && \
                    mkdir "\$_dispatch_reaper_guard_path" 2>/dev/null; then
                    _dispatch_reaper_guard_held=1
                    if ! printf 'pid=%s\n' "\$\$" >"\$_dispatch_reaper_guard_path/owner"; then
                        _dispatch_reaper_guard_release || true
                        _dispatch_reaper_mutex_available=0
                        echo 'remote_agent: dispatch reaper mutex owner record failed' >&2
                    else
                        _guard_recovered=1
                        trap '_dispatch_reaper_guard_release' EXIT
                    fi
                else
                    _dispatch_reaper_mutex_available=0
                fi
                if [ "\$_dispatch_reaper_mutex_available" -eq 0 ]; then
                    if [ -d "\$_dispatch_reaper_guard_path" ]; then
                        _guard_reason=sweep_owner_busy
                    else
                        _guard_reason=sweep_owner_error
                    fi
                    echo "remote_agent: dispatch reaper skipped: \$_guard_reason" >&2
                    printf '{"key":"${LANE_KEY}","outcome":"skipped","reason":"%s","actor":"dispatch_ttl","at":%s}\n' \
                        "\$_guard_reason" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: dispatch reaper skip journal failed for ${LANE_KEY}" >&2
                fi
            fi
        fi
        if [ "\$_dispatch_reaper_mutex_available" -eq 1 ]; then
        # Portable mtime: GNU stat -c %Y, else BSD stat -f %m. Nonzero exit OR
        # non-numeric capture is a silent no-op risk [AGT-10] — warn once on
        # stderr, then skip entry. Validate digits before arithmetic so an
        # empty/garbage exit-0 does not become epoch-scale age and delete live data.
        _reap_mtime_warned=
        _reap_mtime() {
            _mt=
            _mtime_rc=0
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                _mt=\$(_dispatch_sweep_timeout stat -c %Y "\$1" 2>/dev/null) || _mtime_rc=\$?
                if [ "\$_mtime_rc" -ne 0 ] && _dispatch_sweep_has_time; then
                    _mtime_rc=0
                    _mt=\$(_dispatch_sweep_timeout stat -f %m "\$1" 2>/dev/null) || _mtime_rc=\$?
                fi
            else
                _mt=\$(stat -c %Y "\$1" 2>/dev/null) || _mt=\$(stat -f %m "\$1" 2>/dev/null) || _mtime_rc=\$?
            fi
            if [ "\$_mtime_rc" -ne 0 ]; then
                if [ -z "\${_reap_mtime_warned:-}" ]; then
                    echo 'remote_agent: dispatch reaper cannot probe mtime (need GNU stat -c %Y or BSD stat -f %m) — sweep degraded' >&2
                    _reap_mtime_warned=1
                fi
                return 1
            fi
            case "\$_mt" in
                ''|*[!0-9]*)
                    if [ -z "\${_reap_mtime_warned:-}" ]; then
                        echo 'remote_agent: dispatch reaper cannot probe mtime (need GNU stat -c %Y or BSD stat -f %m) — sweep degraded' >&2
                        _reap_mtime_warned=1
                    fi
                    return 1
                    ;;
            esac
            return 0
        }
        # Lane-lock probe [DIAG-07][RES-13][RES-10]: the owning remote shell holds
        # an exclusive lock on \$ROOT/.lane-lock-<key> for its entire lifetime.
        # That is a strictly stronger liveness signal than the occupancy lease
        # (no clock, cannot go stale). Reapers require BOTH lease-not-live AND
        # lock-not-held before deleting. Contract: absent lock file → not held;
        # non-blocking file+cmd probe (self-releasing, never create without a
        # prior existence check); any non-clean probe answer → HELD (fail-safe),
        # matching the malformed-lease arm of _lane_occupant_live.
        _lane_lock_held() {
            _lk="\${1:-}"
            [ -n "\$_lk" ] || return 1
            _llf="\$ROOT/.lane-lock-\${_lk}"
            # No lock file → not held (common case). Must not open/create via probe.
            if [ ! -f "\$_llf" ]; then
                return 1
            fi
            # Only a clean non-blocking acquire proves free; anything else is HELD.
            if flock -n "\$_llf" true 2>/dev/null; then
                return 1
            fi
            return 0
        }
        _lane_is_merged_marked() {
            _lk="\${1:-}"
            [ -n "\$_lk" ] || return 1
            _mf="\$ROOT/.merged/\${_lk}.json"
            [ -s "\$_mf" ]
        }
        _PROC_CWD_SNAPSHOT=
        _PROC_CWD_TABLE=
        _PROC_CWD_SNAPSHOT_READY=0
        _PROC_CWD_PROBE_REASON=
        _lane_proc_root() {
            printf '%s' "\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
        }
        _lane_proc_root_available() {
            _pr="\$(_lane_proc_root)"
            if [ ! -d "\$_pr" ] || [ ! -r "\$_pr" ] || [ ! -x "\$_pr" ]; then
                _PROC_CWD_PROBE_REASON=proc_root_unavailable
                return 1
            fi
            return 0
        }
        _lane_cwd_points_at() {
            _target="\${1:-}"
            _dir="\${2:-}"
            [ -n "\$_target" ] && [ -n "\$_dir" ] || return 1
            case "\$_dir" in
                "\$ROOT"|"\$ROOT"/*) ;;
                *) return 1 ;;
            esac
            case "\$_target" in
                "\$_dir"|"\$_dir"/*|"\$_dir (deleted)"|"\$_dir/"*" (deleted)")
                    case "\$_target" in
                        "\$ROOT"|"\$ROOT"/*|"\$ROOT (deleted)"|"\$ROOT/"*" (deleted)")
                            return 0
                            ;;
                    esac
                    ;;
            esac
            return 1
        }
        _lane_pid_owned_by_self() {
            _pid="\${1:-}"
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            [ -n "\$_pid" ] && [ -O "\$_pr/\$_pid" ]
        }

        _lane_pid_start_identity() {
            _pid="\${1:-}"
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            _LANE_PID_START_IDENTITY=
            [ -n "\$_pid" ] || return 1
            _stat_line=
            { IFS= read -r _stat_line <"\$_pr/\$_pid/stat"; } 2>/dev/null || return 1
            if [[ "\$_stat_line" =~ \)[[:space:]](.*)$ ]]; then
                _stat_rest="\${BASH_REMATCH[1]}"
            else
                return 1
            fi
            _stat_field=0
            for _field in \$_stat_rest; do
                _stat_field=\$((\$_stat_field + 1))
                if [ "\$_stat_field" -eq 20 ]; then
                    _LANE_PID_START_IDENTITY="\$_field"
                    return 0
                fi
            done
            return 1
        }

        _lane_pid_still_in_dir() {
            _pid="\${1:-}"
            _dir="\${2:-}"
            _expected_target="\${3:-}"
            _expected_start="\${4:-}"
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            [ -d "\$_pr/\$_pid" ] || return 1
            _lane_pid_owned_by_self "\$_pid" || return 1
            if [ -n "\$_expected_start" ]; then
                _lane_pid_start_identity "\$_pid" || return 1
                [ "\${_LANE_PID_START_IDENTITY:-}" = "\$_expected_start" ] || return 1
            fi
            _target=\$(readlink "\$_pr/\$_pid/cwd" 2>/dev/null || true)
            if [ -n "\$_expected_target" ]; then
                case "\$_expected_target" in
                    "\${ROOT}/"*" (deleted)") ;;
                    *) return 1 ;;
                esac
                [ "\$_target" = "\$_expected_target" ]
                return
            fi
            _lane_cwd_points_at "\$_target" "\$_dir"
        }
        _lane_collect_matching_pids() {
            _mode="\${1:-}"
            _dir="\${2:-}"
            if [ "\${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
                _snapshot_proc_cwds || return 1
            fi
            _self="\${EUID:-\${UID:-}}"
            _pids=""
            _records=""
            while IFS=\$'\t' read -r _pid _uid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                case "\$_self" in ''|*[!0-9]*) continue ;; esac
                [ "\$_uid" = "\$_self" ] || continue
                [ -n "\$_target" ] || continue
                if [ "\$_mode" = "deleted_root" ]; then
                    case "\$_target" in
                        "\$ROOT/"*" (deleted)") ;;
                        *) continue ;;
                    esac
                    _records="\${_records}\${_pid}"\$'\t'"\${_target}"\$'\t'"\${_start}"\$'\n'
                else
                    _lane_cwd_points_at "\$_target" "\$_dir" || continue
                    _pids="\${_pids} \${_pid}"
                fi
            done <<< "\${_PROC_CWD_TABLE}"
            if [ "\$_mode" = "deleted_root" ]; then
                printf '%s' "\$_records"
            else
                printf '%s' "\$_pids"
            fi
        }
        _lane_terminate_pid_list() {
            _dir="\${1:-}"
            shift
            _pids="\$*"
            [ -n "\$_pids" ] || return 0
            _poll="\${WORKBAY_REMOTE_AGENT_STOP_POLL_SEC:-10}"
            case "\$_poll" in ''|*[!0-9]*) _poll=10 ;; esac
            [ "\$_poll" -le 60 ] || _poll=60
            local _stop_epoch _stop_seconds
            _stop_epoch="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
            _stop_seconds=
            if [ -n "\$_stop_epoch" ]; then
                _stop_seconds=\$((SECONDS + _stop_epoch - \$(date +%s)))
            fi
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                if _remaining="\$(_dispatch_sweep_remaining)"; then
                    [ "\$_remaining" -lt "\$_poll" ] && _poll="\$_remaining"
                else
                    return 1
                fi
            fi
            for _pid in \$_pids; do
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                    env kill -s TERM "\$_pid" 2>/dev/null || true
                fi
            done
            _waited=0
            while [ "\$_waited" -lt "\$_poll" ]; do
                _still=0
                for _pid in \$_pids; do
                    if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                        _still=1
                        break
                    fi
                done
                [ "\$_still" -eq 0 ] && break
                sleep 1
                _waited=\$((_waited + 1))
            done
            for _pid in \$_pids; do
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                    env kill -s KILL "\$_pid" 2>/dev/null || true
                fi
            done
            for _pid in \$_pids; do
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                    return 1
                fi
            done
            return 0
        }

        _lane_terminate_deleted_pid_records() {
            _records="\${1:-}"
            [ -n "\$_records" ] || return 0
            _poll="\${WORKBAY_REMOTE_AGENT_STOP_POLL_SEC:-10}"
            case "\$_poll" in ''|*[!0-9]*) _poll=10 ;; esac
            [ "\$_poll" -le 60 ] || _poll=60
            local _stop_epoch _stop_seconds
            _stop_epoch="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
            _stop_seconds=
            if [ -n "\$_stop_epoch" ]; then
                _stop_seconds=\$((SECONDS + _stop_epoch - \$(date +%s)))
            fi
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                if _remaining="\$(_dispatch_sweep_remaining)"; then
                    [ "\$_remaining" -lt "\$_poll" ] && _poll="\$_remaining"
                else
                    return 1
                fi
            fi
            while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    env kill -s TERM "\$_pid" 2>/dev/null || true
                fi
            done <<< "\$_records"
            _waited=0
            while [ "\$_waited" -lt "\$_poll" ]; do
                _still=0
                while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                    [ -n "\$_pid" ] || continue
                    if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                        _still=1
                        break
                    fi
                done <<< "\$_records"
                [ "\$_still" -eq 0 ] && break
                sleep 1
                _waited=\$((\$_waited + 1))
            done
            while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    env kill -s KILL "\$_pid" 2>/dev/null || true
                fi
            done <<< "\$_records"
            while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                if [ -n "\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}" ]; then
            if [ "\$SECONDS" -ge "\$_stop_seconds" ]; then
                _LANE_STOP_REASON=stop_budget_exhausted
                return 1
            fi
        fi
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    return 1
                fi
            done <<< "\$_records"
            return 0
        }

        _lane_stop_processes() {
            _lane_stop_deadline_remaining() {
                local _deadline _now
                _deadline="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
                case "\$_deadline" in ''|*[!0-9]*) printf 60; return 0 ;; esac
                _now=\$(date +%s)
                if [ "\$_now" -ge "\$_deadline" ]; then
                    _LANE_STOP_REASON=stop_budget_exhausted
                    return 1
                fi
                printf '%s' "\$((_deadline - _now))"
            }

            _lane_stop_systemctl() {
                local _timeout
                _timeout="\$(_lane_stop_deadline_remaining)" || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
                [ "\$_timeout" -le 20 ] || _timeout=20
                if command -v timeout >/dev/null 2>&1; then
                    timeout "\$_timeout" systemctl --user "\$@" 2>/dev/null || true
                else
                    systemctl --user "\$@" 2>/dev/null || true
                fi
                _lane_stop_deadline_remaining >/dev/null || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
            }
            _LANE_STOP_REASON=
            _LANE_STOP_DEADLINE=\$((\$(date +%s) + 60))
            for _budget in "\${reap_deadline:-}" "\${_DISPATCH_SWEEP_DEADLINE:-}"; do
                case "\$_budget" in ''|*[!0-9]*) continue ;; esac
                [ "\$_budget" -ge "\$_LANE_STOP_DEADLINE" ] || _LANE_STOP_DEADLINE="\$_budget"
            done
            _key="\${1:-}"
            _dir="\${2:-}"
            [ -n "\$_key" ] && [ -n "\$_dir" ] || return 1
            case "\$_dir" in
                "\$ROOT"|"\$ROOT"/*) ;;
                *) return 1 ;;
            esac
            _lane_proc_root_available || return 1
            if [ "\${_PROC_CWD_SNAPSHOT_READY:-0}" -ne 1 ]; then
                _snapshot_proc_cwds || return 1
            fi
            for _unit_prefix in workbay-lane grok-lane; do
                _lane_stop_systemctl stop "\${_unit_prefix}-\${_key}.scope" || return 1
                _lane_stop_systemctl stop "\${_unit_prefix}-\${_key}-sv.scope" || return 1
            done
            _job_list="\$(_lane_stop_systemctl list-units --plain --no-legend \
                "workbay-lane-\${_key}-job-*" "grok-lane-\${_key}-job-*" 2>/dev/null)" || { _LANE_STOP_REASON=stop_budget_exhausted; return 1; }
            if [ -n "\$_job_list" ]; then
                _old_ifs="\$IFS"
                IFS=\$'\n'
                for _job_line in \$_job_list; do
                    IFS="\$_old_ifs"
                    _job_unit="\${_job_line%% *}"
                    [ -n "\$_job_unit" ] || continue
                    _lane_stop_systemctl stop "\$_job_unit" || return 1
                done
                IFS="\$_old_ifs"
            fi
            _pids="\$(_lane_collect_matching_pids sandbox "\$_dir")" || return 1
            # shellcheck disable=SC2086
            _lane_terminate_pid_list "\$_dir" \$_pids
        }
        _lane_reap_deleted_cwd_orphans() {
            _records="\$(_lane_collect_matching_pids deleted_root)" || {
                echo "remote_agent: deleted-cwd occupancy probe unknown (\${_PROC_CWD_PROBE_REASON:-proc_probe_failed}); orphan cleanup deferred" >&2
                printf '%s' 0
                return 0
            }
            _killed=0
            if [ -n "\$_records" ]; then
                _before=0
                while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                    [ -n "\$_pid" ] || continue
                    _before=\$((_before + 1))
                done <<< "\$_records"
                _lane_terminate_deleted_pid_records "\$_records" || true
                _after=0
                _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
                while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                    [ -n "\$_pid" ] || continue
                    if [ -d "\$_pr/\$_pid" ]; then
                        _after=\$((_after + 1))
                    fi
                done <<< "\$_records"
                _killed=\$((_before - _after))
                [ "\$_killed" -ge 0 ] || _killed=0
            fi
            printf '%s' "\$_killed"
        }
        _dispatch_proc_dir_uid() {
            _dpu_dir="\${1:-}"
            [ -n "\$_dpu_dir" ] || return 1
            _dpu_rc=0
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                _dpu_uid="\$(_dispatch_sweep_timeout stat -c %u -- "\$_dpu_dir" 2>/dev/null)" || _dpu_rc=\$?
                if [ "\$_dpu_rc" -ne 0 ] && _dispatch_sweep_has_time; then
                    _dpu_rc=0
                    _dpu_uid="\$(_dispatch_sweep_timeout stat -f %u "\$_dpu_dir" 2>/dev/null)" || _dpu_rc=\$?
                fi
            else
                _dpu_uid="\$(stat -c %u -- "\$_dpu_dir" 2>/dev/null)" || _dpu_rc=\$?
                if [ "\$_dpu_rc" -ne 0 ]; then
                    _dpu_rc=0
                    _dpu_uid="\$(stat -f %u "\$_dpu_dir" 2>/dev/null)" || _dpu_rc=\$?
                fi
            fi
            [ "\$_dpu_rc" -eq 0 ] || return 1
            case "\$_dpu_uid" in ''|*[!0-9]*) return 1 ;; esac
            printf '%s' "\$_dpu_uid"
        }
        _snapshot_proc_cwds() {
            local _pr _pid_dir _pid _uid _target _start _cwd _i _output _batch_ok _batch_misaligned _owned_i _n_dirs _n_uids _n_owned _n_targets
            local _identity_uid _identity_uid_rc _identity_start _identity_state _identity_target _identity_target_rc _target_checked
            local _batch_rc
            local _probe_deadline _probe_seconds _stat_uid_mode _stat_rc _stat_probe _batch_valid
            local _retry_root _retry_rc _stat_line _stat_errors _stat_error_path _live_rows _error_rows _batch_uid_count
            local _output_index _fallback_index _fallback_rc _fallback_timeout _fallback_line _fallback_errors _fallback_pid _fallback_error_pid
            local _uncertain_count _fallback_count _fallback_uid_count
            local -a _dirs _uids _starts _owned_cwds _owned_pids _targets _full_targets _target_read _verified_states
            local -a _uncertain_dirs _uncertain_indices _batch_uids _fallback_dirs _fallback_indices _fallback_uids _fallback_stat_args
            if [ "\${_PROC_CWD_RETRY_ACTIVE:-0}" != 1 ]; then
                _PROC_CWD_RETRY_ACTIVE=1
                _PROC_CWD_RETRY_COUNT=0
                _retry_root=1
            else
                _retry_root=0
            fi
            _PROC_CWD_SNAPSHOT=""
            _PROC_CWD_TABLE=""
            _PROC_CWD_UNCLASSIFIED_ROWS=
            _PROC_CWD_SNAPSHOT_READY=0
            _PROC_CWD_PROBE_REASON=
            _PROC_CWD_EXAMINED=0
            _PROC_CWD_OWN_UID=0
            _PROC_CWD_FOREIGN_UNREADABLE=0
            _PROC_CWD_ROOT_UNREADABLE=0
            _PROC_CWD_VANISHED=0
            _PROC_CWD_ZOMBIE=0
            _PROC_CWD_SESSION_INFRA_UNREADABLE=0
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            _proc_cwd_pid_stat() {
                local _ps_line _ps_pattern _ps_pid _ps_rest _ps_field _ps_field_index
                _PROC_CWD_PID_COMM=unknown
                _PROC_CWD_PID_STATE=unknown
                _PROC_CWD_PID_START=
                { IFS= read -r _ps_line < "\$_pr/\$1/stat"; } 2>/dev/null || return 1
                _ps_pid="\${_ps_line%% *}"
                if [ "\$_ps_pid" != "\$1" ]; then _PROC_CWD_PID_COMM=unknown; _PROC_CWD_PID_STATE=; _PROC_CWD_PID_START=; _PROC_CWD_PROBE_REASON=identity_mismatch; return 1; fi
                _ps_pattern='^[0-9]+ \((.*)\) ([A-Za-z]) (.*)\$'
                [[ "\$_ps_line" =~ \$_ps_pattern ]] || return 1
                _PROC_CWD_PID_COMM="\${BASH_REMATCH[1]}"
                _PROC_CWD_PID_STATE="\${BASH_REMATCH[2]}"
                _ps_rest="\${BASH_REMATCH[2]} \${BASH_REMATCH[3]}"
                _ps_field_index=0
                for _ps_field in \$_ps_rest; do
                    _ps_field_index=\$((_ps_field_index + 1))
                    if [ "\$_ps_field_index" -eq 20 ]; then
                        _PROC_CWD_PID_START="\$_ps_field"
                        break
                    fi
                done
                case "\$_PROC_CWD_PID_START" in ''|*[!0-9]*) return 1 ;; esac
            }
            _proc_cwd_probe_report_failure() {
                local _report_pid="\${1:-unknown}" _report_comm=unknown _report_state=unknown _report_uid=unknown
                case "\$_report_pid" in
                    ''|*[!0-9]*) ;;
                    *)
                        if _proc_cwd_pid_stat "\$_report_pid"; then
                            _report_comm="\$_PROC_CWD_PID_COMM"
                            _report_state="\$_PROC_CWD_PID_STATE"
                        fi
                        _report_uid=\$(stat -c %u -- "\$_pr/\$_report_pid" 2>/dev/null) || true
                        case "\$_report_uid" in ''|*[!0-9]*) _report_uid=unknown ;; esac
                        ;;
                esac
                _PROC_CWD_FAILURE_PID="\$_report_pid"
                _PROC_CWD_FAILURE_COMM="\$(printf '%s' "\$_report_comm" | LC_ALL=C tr -cd 'A-Za-z0-9_.:-')"
                [ -n "\$_PROC_CWD_FAILURE_COMM" ] || _PROC_CWD_FAILURE_COMM=unknown
                _PROC_CWD_FAILURE_UID="\$_report_uid"
                echo "remote_agent: proc cwd probe failed pid=\$_report_pid comm=\$_report_comm state=\$_report_state reason=\${_PROC_CWD_PROBE_REASON:-proc_probe_failed} examined=\${_PROC_CWD_EXAMINED} own_uid=\${_PROC_CWD_OWN_UID} foreign_uid_unreadable=\${_PROC_CWD_FOREIGN_UNREADABLE} root_unreadable=\${_PROC_CWD_ROOT_UNREADABLE} session_infra_unreadable=\${_PROC_CWD_SESSION_INFRA_UNREADABLE} vanished=\${_PROC_CWD_VANISHED} zombie=\${_PROC_CWD_ZOMBIE}" >&2
            }
            _proc_cwd_retry_failure() {
                local _retry_deadline _retry_now _retry_left _retry_wait _retry_can_pause=1
                if [ "\${_PROC_CWD_RETRY_COUNT:-0}" -lt 2 ]; then
                    _retry_deadline="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
                    if [ -n "\$_retry_deadline" ]; then
                        case "\$_retry_deadline" in
                            ''|*[!0-9]*) _retry_can_pause=0 ;;
                            *)
                                _retry_now=\$(date +%s)
                                _retry_left=\$((_retry_deadline - _retry_now))
                                [ "\$_retry_left" -gt 1 ] || _retry_can_pause=0
                                ;;
                        esac
                    fi
                    if [ "\$_retry_can_pause" -eq 1 ]; then
                        _PROC_CWD_RETRY_COUNT=\$((_PROC_CWD_RETRY_COUNT + 1))
                        _retry_wait="0.0\$((5 + RANDOM % 10))"
                        sleep "\$_retry_wait"
                        _snapshot_proc_cwds
                        _retry_rc=\$?
                        if [ "\$_retry_root" -eq 1 ]; then
                            unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                        fi
                        return "\$_retry_rc"
                    fi
                fi
                if [ "\$_retry_root" -eq 1 ]; then
                    unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                fi
                return 1
            }
            _dispatch_sweep_record_proc_unknown() {
                local _unknown_pid="\${1:-unknown}" _unknown_uid="\${2:-unknown}" _unknown_reason="\${3:-proc_cwd_probe_failed}" _unknown_comm=unknown
                local _existing_pid _existing_uid _existing_comm _existing_reason
                while IFS=\$'\t' read -r _existing_pid _existing_uid _existing_comm _existing_reason || [ -n "\${_existing_pid:-}" ]; do
                    [ -n "\$_existing_pid" ] || continue
                    [ "\$_existing_pid" != "\$_unknown_pid" ] || return 0
                done <<< "\${_PROC_CWD_UNCLASSIFIED_ROWS:-}"
                case "\$_unknown_pid" in
                    ''|*[!0-9]*) ;;
                    *)
                        if _proc_cwd_pid_stat "\$_unknown_pid"; then
                            _unknown_comm="\$_PROC_CWD_PID_COMM"
                        fi
                        ;;
                esac
                _unknown_comm="\$(printf '%s' "\$_unknown_comm" | LC_ALL=C tr -cd 'A-Za-z0-9_.:-')"
                [ -n "\$_unknown_comm" ] || _unknown_comm=unknown
                case "\$_unknown_uid" in ''|*[!0-9]*) _unknown_uid=unknown ;; esac
                _PROC_CWD_UNCLASSIFIED_ROWS="\${_PROC_CWD_UNCLASSIFIED_ROWS:-}\${_unknown_pid}"\$'\t'"\${_unknown_uid}"\$'\t'"\${_unknown_comm}"\$'\t'"\${_unknown_reason}"\$'\n'
            }
            _dispatch_sweep_mark_unknown_identity() {
                local _mark_index="\${1:-0}" _mark_pid="\${2:-unknown}" _mark_uid="\${3:-unknown}" _mark_reason="\${4:-proc_cwd_probe_failed}"
                _dispatch_sweep_record_proc_unknown "\$_mark_pid" "\$_mark_uid" "\$_mark_reason"
                _uids[\$_mark_index]=
                _full_targets[\$_mark_index]=
                _target_read[\$_mark_index]=1
                _verified_states[\$_mark_index]=proc_probe_unknown
            }
            _proc_cwd_session_infra() {
                local _sid _stat _comm _rest _state _ppid _arg _user _argc _status _key _ruid _extra
                _sid="\$1"
                IFS= read -r _stat < "\$_pr/\$_sid/stat" || return 1
                local _pattern="^[0-9]+ \\((.*)\\) (.*)\$"
                [[ "\$_stat" =~ \$_pattern ]] || return 1
                _comm="\${BASH_REMATCH[1]}"
                _rest="\${BASH_REMATCH[2]}"
                read -r _state _ppid _extra <<< "\$_rest"
                case "\$_ppid" in ""|*[!0-9]*) return 1 ;; esac
                if [ "\$_comm" = gpg-agent ]; then
                    [ "\${2:-}" != systemd ] || return 1
                    local _start= _field _field_index=0 _euid _suid _fsuid _cgroup _supervised=0
                    for _field in \$_rest; do
                        _field_index=\$((_field_index + 1))
                        if [ "\$_field_index" -eq 20 ]; then _start="\$_field"; break; fi
                    done
                    case "\$_start" in ''|*[!0-9]*) return 1 ;; esac
                    _status="\$(cat "\$_pr/\$_sid/status" 2>/dev/null)" || return 1
                    _ruid=
                    while read -r _key _arg _euid _suid _fsuid _extra; do
                        if [ "\$_key" = Uid: ]; then
                            [ -z "\$_ruid" ] && [ -z "\$_extra" ] || return 1
                            _ruid="\$_arg"
                            [ "\$_ruid" = "\$EUID" ] && [ "\$_euid" = "\$EUID" ] &&
                                [ "\$_suid" = "\$EUID" ] && [ "\$_fsuid" = "\$EUID" ] || return 1
                        fi
                    done <<< "\$_status"
                    [ "\$_ruid" = "\$EUID" ] || return 1
                    # Preserve trailing newlines so only the exact single cgroup line qualifies.
                    _cgroup="\$(cat "\$_pr/\$_sid/cgroup" 2>/dev/null && printf '.')" || return 1
                    [ "\$_cgroup" = "0::/user.slice/user-\$EUID.slice/user@\$EUID.service/app.slice/gpg-agent.service"\$'\n.' ] || return 1
                    [ -r "\$_pr/\$_sid/cmdline" ] || return 1
                    while IFS= read -r -d "" _arg; do
                        [ "\$_arg" != --supervised ] || _supervised=1
                    done < "\$_pr/\$_sid/cmdline"
                    [ "\$_supervised" = 1 ] && [ -z "\$_arg" ] || return 1
                    _proc_cwd_session_infra "\$_ppid" systemd || return 1
                    _proc_cwd_pid_stat "\$_sid" || return 1
                    [ "\$_PROC_CWD_PID_START" = "\$_start" ] && [ "\$_PROC_CWD_PID_COMM" = "\$_comm" ]
                    return \$?
                fi
                _user=0
                _argc=0
                [ -r "\$_pr/\$_sid/cmdline" ] || return 1
                while IFS= read -r -d "" _arg; do
                    _argc=\$((_argc + 1))
                    [ "\$_arg" != "--user" ] || _user=1
                done < "\$_pr/\$_sid/cmdline"
                [ "\$_argc" -gt 0 ] || return 1
                _status="\$(cat "\$_pr/\$_ppid/status" 2>/dev/null)" || return 1
                _ruid=
                while read -r _key _arg _extra; do
                    [ "\$_key" != "Uid:" ] || _ruid="\$_arg"
                done <<< "\$_status"
                case "\$_ruid" in ""|*[!0-9]*) return 1 ;; esac
                if [ "\${2:-}" = systemd ] && [ "\$_comm" != systemd ]; then
                    return 1
                fi
                case "\$_comm" in
                    systemd) [ "\$_user" -eq 1 ] && [ "\$_ruid" = 0 ] ;;
                    "(sd-pam)") _proc_cwd_session_infra "\$_ppid" systemd ;;
                    tailscaled|sshd) [ "\$_ruid" = 0 ] ;;
                    *) return 1 ;;
                esac
            }
            _proc_cwd_readlink() {
                local _left
                if [ -n "\$_probe_seconds" ]; then
                    _left=\$((_probe_seconds - SECONDS))
                    [ "\$_left" -gt 0 ] || return 124
                    if command -v timeout >/dev/null 2>&1; then
                        timeout "\$_left" readlink "\$@"
                    else
                        readlink "\$@"
                    fi
                else
                    readlink "\$@"
                fi
            }
            _proc_cwd_deadline_expired() {
                local _deadline _now
                _deadline="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
                case "\$_deadline" in ''|*[!0-9]*) return 1 ;; esac
                _now=\$(date +%s)
                if [ "\$_now" -ge "\$_deadline" ]; then
                    _PROC_CWD_PROBE_REASON=deadline
                    return 0
                fi
                return 1
            }
            _proc_cwd_identity_uid() {
                local _uid_deadline _uid_timeout
                _uid_deadline="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
                case "\$_uid_deadline" in
                    '')
                        if [ "\$_stat_uid_mode" = gnu ]; then
                            stat -c %u -- "\$1" 2>/dev/null
                        else
                            stat -f %u "\$1" 2>/dev/null
                        fi
                        ;;
                    *[!0-9]*) return 124 ;;
                    *)
                        _uid_timeout=\$(( _uid_deadline - \$(date +%s) ))
                        [ "\$_uid_timeout" -gt 0 ] || return 124
                        command -v timeout >/dev/null 2>&1 || return 2
                        if [ "\$_stat_uid_mode" = gnu ]; then
                            timeout "\$_uid_timeout" stat -c %u -- "\$1" 2>/dev/null
                        else
                            timeout "\$_uid_timeout" stat -f %u "\$1" 2>/dev/null
                        fi
                        ;;
                esac
            }
            if ! _lane_proc_root_available; then
                _PROC_CWD_PROBE_REASON=proc_root_unavailable
                _proc_cwd_probe_report_failure unknown
                _proc_cwd_retry_failure
                return \$?
            fi
            _probe_deadline="\${_LANE_STOP_DEADLINE:-\${_DISPATCH_SWEEP_DEADLINE:-\${reap_deadline:-}}}"
            _probe_seconds=
            if [ -n "\$_probe_deadline" ]; then
                _probe_seconds=\$((SECONDS + _probe_deadline - \$(date +%s)))
            fi
            _dirs=()
            _n_dirs=0
            for _pid_dir in "\$_pr"/[0-9]*; do
                [ -d "\$_pid_dir" ] || continue
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _dirs+=("\$_pid_dir")
                _n_dirs=\$((_n_dirs + 1))
            done
            _starts=()
            _i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                if _proc_cwd_pid_stat "\$_pid"; then
                    _starts+=("\$_PROC_CWD_PID_START")
                elif [ ! -d "\$_pid_dir" ] || { [ ! -e "\$_pid_dir/stat" ] && [ ! -L "\$_pid_dir/stat" ]; }; then
                    _starts+=("")
                else
                    # UID collection below decides whether this stat failure is
                    # foreign and safe to skip or own and therefore fail closed.
                    _starts+=("")
                fi
                _i=\$((_i + 1))
            done
            _uids=()
            _n_uids=0
            if [ "\$_n_dirs" -gt 0 ]; then
                _stat_uid_mode=
                if _stat_probe="\$(stat -c %u -- "\$_pr" 2>/dev/null)" && [[ "\$_stat_probe" =~ ^[0-9]+\$ ]]; then
                    _stat_uid_mode=gnu
                elif _stat_probe="\$(stat -f %u "\$_pr" 2>/dev/null)" && [[ "\$_stat_probe" =~ ^[0-9]+\$ ]]; then
                    _stat_uid_mode=bsd
                else
                    _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                    _pid=unknown
                    if [[ "\${_dirs[0]}" =~ /([0-9]+)\$ ]]; then
                        _pid="\${BASH_REMATCH[1]}"
                    fi
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                _stat_rc=0
                if [ "\$_stat_uid_mode" = gnu ]; then
                    _output=\$(stat -c %u -- "\${_dirs[@]}" 2>&1) || _stat_rc=\$?
                else
                    _output=\$(stat -f %u "\${_dirs[@]}" 2>&1) || _stat_rc=\$?
                fi
                _uids=()
                _n_uids=0
                _stat_errors=
                while IFS= read -r _stat_line || [ -n "\$_stat_line" ]; do
                    case "\$_stat_line" in
                        '' ) ;;
                        *[!0-9]*) _stat_errors="\${_stat_errors}\${_stat_line}"\$'\n' ;;
                        *)
                            _uids+=("\$_stat_line")
                            _n_uids=\$((_n_uids + 1))
                            ;;
                    esac
                done <<< "\$_output"
                _batch_valid=1
                [ "\$_stat_rc" -eq 0 ] && [ "\$_n_uids" -eq "\$_n_dirs" ] && [ -z "\$_stat_errors" ] || _batch_valid=0
                if [ "\$_batch_valid" -eq 0 ]; then
                    # Keep rows named by stat diagnostics and retry only those rows.
                    # If a silent/malformed batch cannot identify the missing rows,
                    # treat its live rows as uncertain and recover them in 256-PID
                    # chunks rather than forking once per process.
                    _batch_uids=(\${_uids[@]+"\${_uids[@]}"})
                    _batch_uid_count="\$_n_uids"
                    _live_rows=0
                    _error_rows=0
                    for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                        [ -d "\$_pid_dir" ] || continue
                        _live_rows=\$((_live_rows + 1))
                        case "\$_stat_errors" in
                            *"\$_pid_dir'"*|*"\$_pid_dir:"*|*"\$_pid_dir"\$'\n'*) _error_rows=\$((_error_rows + 1)) ;;
                        esac
                    done
                    _uids=()
                    _uncertain_dirs=()
                    _uncertain_indices=()
                    _uncertain_count=0
                    if { [ "\$_error_rows" -gt 0 ] || [ "\$_live_rows" -lt "\$_n_dirs" ]; } && \
                        [ "\$_batch_uid_count" -eq "\$((_live_rows - _error_rows))" ]; then
                        _output_index=0
                        _i=0
                        for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                            if [ ! -d "\$_pid_dir" ]; then
                                _uids+=("")
                            else
                                case "\$_stat_errors" in
                                    *"\$_pid_dir'"*|*"\$_pid_dir:"*|*"\$_pid_dir"\$'\n'*)
                                        _uids+=("")
                                        _uncertain_dirs+=("\$_pid_dir")
                                        _uncertain_indices+=("\$_i")
                                        _uncertain_count=\$((_uncertain_count + 1))
                                        ;;
                                    *)
                                        _uids+=("\${_batch_uids[\$_output_index]:-}")
                                        _output_index=\$((_output_index + 1))
                                        ;;
                                esac
                            fi
                            _i=\$((_i + 1))
                        done
                    else
                        _i=0
                        for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                            _uids+=("")
                            if [ -d "\$_pid_dir" ]; then
                                _uncertain_dirs+=("\$_pid_dir")
                                _uncertain_indices+=("\$_i")
                                _uncertain_count=\$((_uncertain_count + 1))
                            fi
                            _i=\$((_i + 1))
                        done
                    fi

                    _fallback_index=0
                    while [ "\$_fallback_index" -lt "\$_uncertain_count" ]; do
                        if _proc_cwd_deadline_expired; then
                            [ "\$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                            return 1
                        fi
                        _fallback_dirs=()
                        _fallback_indices=()
                        _fallback_count=0
                        while [ "\$_fallback_index" -lt "\$_uncertain_count" ] && [ "\$_fallback_count" -lt 256 ]; do
                            _pid_dir="\${_uncertain_dirs[\$_fallback_index]}"
                            _i="\${_uncertain_indices[\$_fallback_index]}"
                            if [ -d "\$_pid_dir" ]; then
                                _fallback_dirs+=("\$_pid_dir")
                                _fallback_indices+=("\$_i")
                                _fallback_count=\$((_fallback_count + 1))
                            else
                                _uids[\$_i]=
                            fi
                            _fallback_index=\$((_fallback_index + 1))
                        done
                        [ "\$_fallback_count" -gt 0 ] || continue
                        _fallback_rc=0
                        _fallback_timeout=
                        case "\$_probe_deadline" in
                            ''|*[!0-9]*) ;;
                            *)
                                _fallback_timeout=\$((\$_probe_deadline - \$(date +%s)))
                                [ "\$_fallback_timeout" -gt 1 ] || _fallback_timeout=1
                                ;;
                        esac
                        if [ "\$_stat_uid_mode" = gnu ]; then
                            _fallback_stat_args=(-c %u --)
                        else
                            _fallback_stat_args=(-f %u)
                        fi
                        if [ -n "\$_fallback_timeout" ]; then
                            if ! command -v timeout >/dev/null 2>&1; then
                                _PROC_CWD_PROBE_REASON=deadline
                                [ "\$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                                return 1
                            fi
                            _output=\$(timeout "\$_fallback_timeout" stat \${_fallback_stat_args[@]+"\${_fallback_stat_args[@]}"} \${_fallback_dirs[@]+"\${_fallback_dirs[@]}"} 2>&1) || _fallback_rc=\$?
                        else
                            _output=\$(stat \${_fallback_stat_args[@]+"\${_fallback_stat_args[@]}"} \${_fallback_dirs[@]+"\${_fallback_dirs[@]}"} 2>&1) || _fallback_rc=\$?
                        fi
                        if [ "\$_fallback_rc" -eq 124 ] || [ "\$_fallback_rc" -eq 137 ]; then
                            _PROC_CWD_PROBE_REASON=deadline
                            [ "\$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                            return 1
                        fi
                        _fallback_uids=()
                        _fallback_uid_count=0
                        _fallback_errors=
                        while IFS= read -r _fallback_line || [ -n "\$_fallback_line" ]; do
                            case "\$_fallback_line" in
                                '' ) ;;
                                *[!0-9]*) _fallback_errors="\${_fallback_errors}\${_fallback_line}"\$'\n' ;;
                                *)
                                    _fallback_uids+=("\$_fallback_line")
                                    _fallback_uid_count=\$((_fallback_uid_count + 1))
                                    ;;
                            esac
                        done <<< "\$_output"
                        _live_rows=0
                        _fallback_pid=unknown
                        _fallback_error_pid=unknown
                        for _pid_dir in \${_fallback_dirs[@]+"\${_fallback_dirs[@]}"}; do
                            if [ -d "\$_pid_dir" ]; then
                                _live_rows=\$((_live_rows + 1))
                                if [[ "\$_pid_dir" =~ /([0-9]+)\$ ]]; then
                                    _fallback_pid="\${BASH_REMATCH[1]}"
                                    case "\$_fallback_errors" in
                                        *"\$_pid_dir'"*|*"\$_pid_dir:"*|*"\$_pid_dir"\$'\n'*) _fallback_error_pid="\${BASH_REMATCH[1]}" ;;
                                    esac
                                fi
                            fi
                        done
                        [ "\$_fallback_error_pid" = unknown ] || _fallback_pid="\$_fallback_error_pid"
                        if [ "\$_fallback_rc" -eq 0 ] && [ "\$_fallback_uid_count" -eq "\$_fallback_count" ] && \
                            [ -z "\$_fallback_errors" ]; then
                            _i=0
                            for _pid_dir in \${_fallback_dirs[@]+"\${_fallback_dirs[@]}"}; do
                                _uids[\${_fallback_indices[\$_i]}]="\${_fallback_uids[\$_i]}"
                                _i=\$((_i + 1))
                            done
                        elif [ "\$_live_rows" -eq 0 ] && [ "\$_fallback_uid_count" -eq 0 ]; then
                            for _i in \${_fallback_indices[@]+"\${_fallback_indices[@]}"}; do _uids[\$_i]=; done
                        else
                            if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                                for _i in \${_fallback_indices[@]+"\${_fallback_indices[@]}"}; do _uids[\$_i]=; done
                                continue
                            fi
                            _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                            _proc_cwd_probe_report_failure "\$_fallback_pid"
                            [ "\$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                            return 1
                        fi
                        if _proc_cwd_deadline_expired; then
                            [ "\$_retry_root" -eq 0 ] || unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                            return 1
                        fi
                    done
                fi
            fi
            _owned_cwds=()
            _owned_pids=()
            _n_owned=0
            _i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                _uid="\${_uids[\$_i]:-}"
                _i=\$((_i + 1))
                [ -d "\$_pid_dir" ] || continue
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                case "\$_uid" in ''|*[!0-9]*)
                    [ -d "\$_pid_dir" ] || continue
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                    _pid=unknown
                    if [[ "\$_pid_dir" =~ /([0-9]+)\$ ]]; then
                        _pid="\${BASH_REMATCH[1]}"
                    fi
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                    ;;
                esac
                if [ "\$_uid" = "\${EUID:-\${UID:-}}" ]; then
                    if [ -z "\${_starts[\$((_i - 1))]:-}" ]; then
                        if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                            continue
                        fi
                        _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                        _proc_cwd_probe_report_failure "\$_pid"
                        _proc_cwd_retry_failure
                        return \$?
                    fi
                    if [ ! -O "\$_pid_dir" ]; then
                        [ -d "\$_pid_dir" ] || continue
                        if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                            continue
                        fi
                        _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                        _pid=unknown
                        if [[ "\$_pid_dir" =~ /([0-9]+)\$ ]]; then
                            _pid="\${BASH_REMATCH[1]}"
                        fi
                    _proc_cwd_probe_report_failure "\$_pid"
                        _proc_cwd_retry_failure
                        return \$?
                    fi
                    if [ -d "\$_pid_dir" ]; then
                        _cwd="\$_pid_dir/cwd"
                        if [ -x "\$_pid_dir" ] && [ ! -e "\$_cwd" ] && [ ! -L "\$_cwd" ]; then
                            continue
                        fi
                        if _proc_cwd_pid_stat "\$_pid" && { [ "\$_PROC_CWD_PID_STATE" = Z ] || [ "\$_PROC_CWD_PID_STATE" = X ]; }; then
                            continue
                        fi
                        [ -d "\$_pid_dir" ] || continue
                        _owned_cwds+=("\$_cwd")
                        _owned_pids+=("\$_pid")
                        _n_owned=\$((_n_owned + 1))
                    fi
                fi
            done
            _targets=()
            _n_targets=0
            _batch_ok=0
            _batch_misaligned=0
            if [ "\$_n_owned" -gt 0 ]; then
                _batch_rc=0
                _output=\$(_proc_cwd_readlink "\${_owned_cwds[@]}" 2>/dev/null) || _batch_rc=\$?
                if [ "\$_batch_rc" -eq 0 ]; then
                    while IFS= read -r _target || [ -n "\$_target" ]; do
                        _targets+=("\$_target")
                        _n_targets=\$((_n_targets + 1))
                    done <<< "\$_output"
                    if [ "\$_n_targets" -ne "\$_n_owned" ]; then
                        _batch_misaligned=1
                    else
                        _batch_ok=1
                    fi
                fi
            fi
            _full_targets=()
            _target_read=()
            _verified_states=()
            _i=0
            _owned_i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                _uid="\${_uids[\$_i]:-}"
                _target=
                _target_checked=0
                if [ "\$_batch_ok" -eq 1 ] && [ "\$_uid" = "\${EUID:-\${UID:-}}" ] && \
                    [ "\${_owned_pids[\$_owned_i]:-}" = "\$_pid" ]; then
                    _target="\${_targets[\$_owned_i]:-}"
                    _target_checked=1
                    _owned_i=\$((_owned_i + 1))
                elif [ "\$_uid" = "\${EUID:-\${UID:-}}" ] && \
                    [ "\${_owned_pids[\$_owned_i]:-}" = "\$_pid" ]; then
                    _owned_i=\$((_owned_i + 1))
                fi
                _full_targets+=("\$_target")
                _target_read+=("\$_target_checked")
                _verified_states+=("")
                _i=\$((_i + 1))
            done

            # When the batch failed, collect individual cwd values before the single
            # identity verification pass so those reads are bound to the same snapshot.
            _i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                _uid="\${_uids[\$_i]:-}"
                if [ "\$_uid" = "\${EUID:-\${UID:-}}" ] && [ "\${_target_read[\$_i]:-0}" != 1 ]; then
                    _cwd="\$_pid_dir/cwd"
                    if [ ! -d "\$_pid_dir" ]; then
                        _target_read[\$_i]=1
                    elif _proc_cwd_pid_stat "\$_pid" && { [ "\$_PROC_CWD_PID_STATE" = Z ] || [ "\$_PROC_CWD_PID_STATE" = X ]; }; then
                        _target_read[\$_i]=1
                    elif [ -x "\$_pid_dir" ] && [ ! -e "\$_cwd" ] && [ ! -L "\$_cwd" ]; then
                        _target_read[\$_i]=1
                    else
                        _identity_target_rc=0
                        _identity_target=\$(_proc_cwd_readlink "\$_cwd" 2>/dev/null) || _identity_target_rc=\$?
                        if [ "\$_identity_target_rc" -eq 0 ]; then
                            _full_targets[\$_i]="\$_identity_target"
                            _target_read[\$_i]=1
                        else
                            _target_read[\$_i]=2
                        fi
                    fi
                fi
                _i=\$((_i + 1))
            done

            # Bind both batched results to the process identity seen at enumeration.
            # A reused PID discards both rows and is sampled once as a new process.
            _i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                if [ ! -d "\$_pid_dir" ]; then
                    _uids[\$_i]=
                    _full_targets[\$_i]=
                    _target_read[\$_i]=1
                    _verified_states[\$_i]=vanished
                    _i=\$((_i + 1))
                    continue
                fi
                if [ "\${_uids[\$_i]:-}" != "\${EUID:-\${UID:-}}" ] && [ -z "\${_starts[\$_i]:-}" ]; then
                    # A failed initial stat leaves no identity to bind the batched UID
                    # to. Re-read both owner and start time before trusting it as foreign.
                    if ! _proc_cwd_pid_stat "\$_pid"; then
                        if [ ! -d "\$_pid_dir" ]; then
                            _uids[\$_i]=
                            _full_targets[\$_i]=
                            _target_read[\$_i]=1
                            _verified_states[\$_i]=vanished
                            _i=\$((_i + 1))
                            continue
                        fi
                        if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                            _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_cwd_probe_failed
                            _i=\$((_i + 1))
                            continue
                        fi
                        _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                        _proc_cwd_probe_report_failure "\$_pid"
                        _proc_cwd_retry_failure
                        return \$?
                    fi
                    _identity_start="\$_PROC_CWD_PID_START"
                    _identity_state="\$_PROC_CWD_PID_STATE"
                    _identity_uid_rc=0
                    _identity_uid=\$(_proc_cwd_identity_uid "\$_pid_dir") || _identity_uid_rc=\$?
                    if [ "\$_identity_uid_rc" -eq 124 ] || [ "\$_identity_uid_rc" -eq 137 ]; then
                        _PROC_CWD_PROBE_REASON=deadline
                        unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                        return 1
                    fi
                    case "\$_identity_uid" in ''|*[!0-9]*) _identity_uid_rc=1 ;; esac
                    if [ "\$_identity_uid_rc" -ne 0 ]; then
                        if [ ! -d "\$_pid_dir" ]; then
                            _uids[\$_i]=
                            _full_targets[\$_i]=
                            _target_read[\$_i]=1
                            _verified_states[\$_i]=vanished
                            _i=\$((_i + 1))
                            continue
                        fi
                        if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                            _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_uid_probe_unknown
                            _i=\$((_i + 1))
                            continue
                        fi
                        _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                        _proc_cwd_probe_report_failure "\$_pid"
                        _proc_cwd_retry_failure
                        return \$?
                    fi
                    _uids[\$_i]="\$_identity_uid"
                    if [ "\$_identity_uid" != "\${EUID:-\${UID:-}}" ]; then
                        if ! _proc_cwd_pid_stat "\$_pid"; then
                            if [ ! -d "\$_pid_dir" ]; then
                                _uids[\$_i]=
                                _full_targets[\$_i]=
                                _target_read[\$_i]=1
                                _verified_states[\$_i]=vanished
                                _i=\$(( _i + 1 ))
                                continue
                            fi
                            if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                                _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" "\$_identity_uid" proc_cwd_probe_failed
                                _i=\$(( _i + 1 ))
                                continue
                            fi
                            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                            _proc_cwd_probe_report_failure "\$_pid"
                            _proc_cwd_retry_failure
                            return \$?
                        fi
                        if [ "\$_PROC_CWD_PID_START" != "\$_identity_start" ]; then
                            if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                                _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" "\$_identity_uid" proc_cwd_probe_failed
                                _i=\$(( _i + 1 ))
                                continue
                            fi
                            _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                            _proc_cwd_probe_report_failure "\$_pid"
                            _proc_cwd_retry_failure
                            return \$?
                        fi
                        _starts[\$_i]="\$_identity_start"
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]="\$_identity_state"
                        _i=\$((_i + 1))
                        continue
                    fi
                fi
                if ! _proc_cwd_pid_stat "\$_pid"; then
                    if [ ! -d "\$_pid_dir" ]; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]=vanished
                        _i=\$((_i + 1))
                        continue
                    fi
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_cwd_probe_failed
                        _i=\$((_i + 1))
                        continue
                    fi
                    _cwd="\$_pid_dir/cwd"
                    if [ -x "\$_pid_dir" ] && [ ! -e "\$_cwd" ] && [ ! -L "\$_cwd" ]; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]=vanished
                        _i=\$((_i + 1))
                        continue
                    fi
                    case "\${_uids[\$_i]:-}" in
                        ''|*[!0-9]*) ;;
                        *)
                            if [ "\${_uids[\$_i]}" != "\${EUID:-\${UID:-}}" ]; then
                                _full_targets[\$_i]=
                                _target_read[\$_i]=1
                                _verified_states[\$_i]=foreign_unverified
                                _i=\$((_i + 1))
                                continue
                            fi
                            ;;
                    esac
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                _identity_start="\$_PROC_CWD_PID_START"
                _identity_state="\$_PROC_CWD_PID_STATE"
                if [ -n "\${_starts[\$_i]:-}" ] && [ "\$_identity_start" = "\${_starts[\$_i]}" ]; then
                    _verified_states[\$_i]="\$_identity_state"
                    _i=\$((_i + 1))
                    continue
                fi

                _identity_uid_rc=0
                _identity_uid=\$(_proc_cwd_identity_uid "\$_pid_dir") || _identity_uid_rc=\$?
                if [ "\$_identity_uid_rc" -eq 124 ] || [ "\$_identity_uid_rc" -eq 137 ]; then
                    _PROC_CWD_PROBE_REASON=deadline
                    unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
                    return 1
                fi
                case "\$_identity_uid" in ''|*[!0-9]*) _identity_uid_rc=1 ;; esac
                if [ "\$_identity_uid_rc" -ne 0 ]; then
                    if [ ! -d "\$_pid_dir" ]; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]=vanished
                        _i=\$((_i + 1))
                        continue
                    fi
                    if _proc_cwd_pid_stat "\$_pid" && { [ "\$_PROC_CWD_PID_STATE" = Z ] || [ "\$_PROC_CWD_PID_STATE" = X ]; }; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]="\$_PROC_CWD_PID_STATE"
                        _i=\$((_i + 1))
                        continue
                    fi
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_uid_probe_unknown
                        _i=\$((_i + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi

                _cwd="\$_pid_dir/cwd"
                _identity_target_rc=0
                _identity_target=\$(_proc_cwd_readlink "\$_cwd" 2>/dev/null) || _identity_target_rc=\$?
                if [ "\$_identity_target_rc" -ne 0 ]; then
                    if [ ! -d "\$_pid_dir" ]; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]=vanished
                        _i=\$((_i + 1))
                        continue
                    fi
                    if _proc_cwd_pid_stat "\$_pid" && { [ "\$_PROC_CWD_PID_STATE" = Z ] || [ "\$_PROC_CWD_PID_STATE" = X ]; }; then
                        _uids[\$_i]="\$_identity_uid"
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]="\$_PROC_CWD_PID_STATE"
                        _i=\$((_i + 1))
                        continue
                    fi
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" "\${_uids[\$_i]:-unknown}" proc_cwd_probe_failed
                        _i=\$((_i + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                if ! _proc_cwd_pid_stat "\$_pid"; then
                    if [ ! -d "\$_pid_dir" ]; then
                        _uids[\$_i]=
                        _full_targets[\$_i]=
                        _target_read[\$_i]=1
                        _verified_states[\$_i]=vanished
                        _i=\$((_i + 1))
                        continue
                    fi
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_cwd_probe_failed
                        _i=\$((_i + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                if [ "\$_PROC_CWD_PID_START" != "\$_identity_start" ]; then
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_mark_unknown_identity "\$_i" "\$_pid" unknown proc_cwd_probe_failed
                        _i=\$((_i + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                _starts[\$_i]="\$_identity_start"
                _uids[\$_i]="\$_identity_uid"
                _full_targets[\$_i]="\$_identity_target"
                _target_read[\$_i]=1
                _verified_states[\$_i]="\$_PROC_CWD_PID_STATE"
                _i=\$((_i + 1))
            done

            if [ "\$_batch_misaligned" -eq 1 ]; then
                _PROC_CWD_PROBE_REASON=batch_misaligned
                _pid="\${_owned_pids[0]:-unknown}"
                if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                    for _pid in \${_owned_pids[@]+"\${_owned_pids[@]}"}; do
                        _dispatch_sweep_record_proc_unknown "\$_pid" "\${EUID:-\${UID:-unknown}}" batch_misaligned
                    done
                else
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
            fi

            _i=0
            for _pid_dir in \${_dirs[@]+"\${_dirs[@]}"}; do
                [[ "\$_pid_dir" =~ /([0-9]+)\$ ]] || continue
                _pid="\${BASH_REMATCH[1]}"
                _PROC_CWD_EXAMINED=\$((_PROC_CWD_EXAMINED + 1))
                _uid="\${_uids[\$_i]:-}"
                _target="\${_full_targets[\$_i]:-}"
                _start="\${_starts[\$_i]:-}"
                _identity_state="\${_verified_states[\$_i]:-}"
                _i=\$((_i + 1))
                if [ "\$_identity_state" = proc_probe_unknown ]; then
                    continue
                fi
                if [ "\$_identity_state" = vanished ]; then
                    _PROC_CWD_VANISHED=\$((_PROC_CWD_VANISHED + 1))
                    continue
                fi
                if [ "\$_identity_state" = Z ] || [ "\$_identity_state" = X ]; then
                    _PROC_CWD_VANISHED=\$((_PROC_CWD_VANISHED + 1))
                    _PROC_CWD_ZOMBIE=\$((_PROC_CWD_ZOMBIE + 1))
                    continue
                fi
                if [ ! -d "\$_pid_dir" ]; then
                    _PROC_CWD_VANISHED=\$((_PROC_CWD_VANISHED + 1))
                    continue
                fi
                case "\$_uid" in ''|*[!0-9]*)
                    [ -d "\$_pid_dir" ] || {
                        _PROC_CWD_VANISHED=\$((_PROC_CWD_VANISHED + 1))
                        continue
                    }
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                        _dispatch_sweep_record_proc_unknown "\$_pid" unknown "\$_PROC_CWD_PROBE_REASON"
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_uid_probe_unknown
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                    ;;
                esac
                if [ "\$_uid" != "\${EUID:-\${UID:-}}" ]; then
                    if [ "\$_uid" = 0 ]; then
                        _PROC_CWD_ROOT_UNREADABLE=\$((_PROC_CWD_ROOT_UNREADABLE + 1))
                    else
                        _PROC_CWD_FOREIGN_UNREADABLE=\$((_PROC_CWD_FOREIGN_UNREADABLE + 1))
                    fi
                    continue
                fi
                _PROC_CWD_OWN_UID=\$((_PROC_CWD_OWN_UID + 1))
                _cwd="\$_pid_dir/cwd"
                if [ -x "\$_pid_dir" ] && [ ! -e "\$_cwd" ] && [ ! -L "\$_cwd" ]; then
                    _PROC_CWD_VANISHED=\$((_PROC_CWD_VANISHED + 1))
                    continue
                fi
                if [ "\${_target_read[\$((_i - 1))]:-0}" != 1 ]; then
                    if [ "\${_target_read[\$((_i - 1))]:-0}" = 2 ] && _proc_cwd_session_infra "\$_pid"; then
                        _PROC_CWD_SESSION_INFRA_UNREADABLE=\$((_PROC_CWD_SESSION_INFRA_UNREADABLE + 1))
                        continue
                    fi
                    _PROC_CWD_PROBE_REASON=proc_cwd_probe_failed
                    if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ]; then
                        _dispatch_sweep_record_proc_unknown "\$_pid" "\$_uid" "\$_PROC_CWD_PROBE_REASON"
                        continue
                    fi
                    _proc_cwd_probe_report_failure "\$_pid"
                    _proc_cwd_retry_failure
                    return \$?
                fi
                _PROC_CWD_SNAPSHOT="\${_PROC_CWD_SNAPSHOT}\${_target}"\$'\n'
                _PROC_CWD_TABLE="\${_PROC_CWD_TABLE}\${_pid}"\$'\t'"\${_uid}"\$'\t'"\${_target}"\$'\t'"\${_start}"\$'\n'
            done
            if ! _lane_proc_root_available; then
                _PROC_CWD_PROBE_REASON=proc_root_unavailable
                _proc_cwd_probe_report_failure unknown
                _proc_cwd_retry_failure
                return \$?
            fi
            echo "remote_agent: proc cwd examined=\${_PROC_CWD_EXAMINED} own_uid=\${_PROC_CWD_OWN_UID} foreign_uid_unreadable=\${_PROC_CWD_FOREIGN_UNREADABLE} root_unreadable=\${_PROC_CWD_ROOT_UNREADABLE} session_infra_unreadable=\${_PROC_CWD_SESSION_INFRA_UNREADABLE} vanished=\${_PROC_CWD_VANISHED} zombie=\${_PROC_CWD_ZOMBIE}" >&2
            _PROC_CWD_SNAPSHOT_READY=1
            if [ "\$_retry_root" -eq 1 ]; then
                unset _PROC_CWD_RETRY_ACTIVE _PROC_CWD_RETRY_COUNT
            fi
            return 0
        }
        _dispatch_sweep_proc_unknown_for_sandbox() {
            local _du_sandbox="\${1:-}" _du_key _du_pid _du_uid _du_comm _du_reason
            local _du_cgroup _du_cgroup_line _du_lane_match
            [ -n "\$_du_sandbox" ] || return 1
            _du_key="\${_du_sandbox##*/}"
            while IFS=\$'\t' read -r _du_pid _du_uid _du_comm _du_reason || [ -n "\${_du_pid:-}" ]; do
                [ -n "\$_du_pid" ] || continue
                case "\$_du_uid" in
                    "\${EUID:-\${UID:-}}"|unknown|'') ;;
                    *) continue ;;
                esac
                _du_lane_match=0
                _du_cgroup="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}/\$_du_pid/cgroup"
                if [ -r "\$_du_cgroup" ]; then
                    while IFS= read -r _du_cgroup_line || [ -n "\$_du_cgroup_line" ]; do
                        case "\$_du_cgroup_line" in
                            *"/workbay-lane-\$_du_key.scope"*|*"/workbay-lane-\$_du_key-sv.scope"*|*"/grok-lane-\$_du_key.scope"*|*"/grok-lane-\$_du_key-sv.scope"*|*"/workbay-lane-\$_du_key-job-"*|*"/grok-lane-\$_du_key-job-"*)
                                _du_lane_match=1
                                ;;
                        esac
                        if [[ "\$_du_cgroup_line" =~ /(workbay-lane|grok-lane)-"\$_du_key"-r[0-9]+(-sv)?\.scope([^/A-Za-z0-9._-]|$) ]]; then
                            _du_lane_match=1
                        fi
                    done < "\$_du_cgroup"
                    # Cgroup membership can prove a candidate is occupied,
                    # but a non-match cannot prove that an unclassified PID
                    # is unrelated: workers may run without a lane scope.
                    [ "\$_du_lane_match" -eq 1 ] || _du_reason=proc_cgroup_ownership_unknown
                else
                    _du_reason=proc_cgroup_probe_failed
                fi
                _PROC_CWD_MATCHED_PID="\$_du_pid"
                _PROC_CWD_MATCHED_COMM="\$_du_comm"
                _PROC_CWD_MATCHED_UID="\$_du_uid"
                _PROC_CWD_PROBE_REASON="\$_du_reason"
                return 0
            done <<< "\${_PROC_CWD_UNCLASSIFIED_ROWS:-}"
            return 1
        }
        _pid_in_sandbox_live() {
            _sd="\${1:-}"
            [ -n "\$_sd" ] || return 0
            _PROC_CWD_PROBE_REASON=
            _snapshot_proc_cwds || return 0
            while IFS=\$'\t' read -r _pid _uid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_target" ] || continue
                case "\$_target" in
                    "\$_sd"|"\$_sd"/*|"\$_sd (deleted)"|"\$_sd/"*" (deleted)") _PROC_CWD_PROBE_REASON=live_pid; return 0 ;;
                esac
            done <<< "\${_PROC_CWD_TABLE}"
            if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ] && _dispatch_sweep_proc_unknown_for_sandbox "\$_sd"; then
                return 0
            fi
            if ! _lane_proc_root_available; then
                return 0
            fi
            return 1
        }
        _pid_in_sandbox() {
            _sd="\${1:-}"
            [ -n "\$_sd" ] || return 0
            _PROC_CWD_PROBE_REASON=
            _lane_proc_root_available || return 0
            if [ "\${_PROC_CWD_SNAPSHOT_READY:-0}" -eq 1 ]; then
                while IFS= read -r _target || [ -n "\$_target" ]; do
                    [ -n "\$_target" ] || continue
                    case "\$_target" in
                        "\$_sd"|"\$_sd"/*|"\$_sd (deleted)"|"\$_sd/"*" (deleted)") _PROC_CWD_PROBE_REASON=live_pid; return 0 ;;
                    esac
                done <<< "\${_PROC_CWD_SNAPSHOT}"
                if [ "\${_DISPATCH_SWEEP_PER_CANDIDATE:-0}" = 1 ] && _dispatch_sweep_proc_unknown_for_sandbox "\$_sd"; then
                    return 0
                fi
                _lane_proc_root_available || return 0
                return 1
            fi
            _pid_in_sandbox_live "\$_sd"
        }
        _dispatch_sweep_remaining() {
            _deadline="\${_DISPATCH_SWEEP_DEADLINE:-}"
            case "\$_deadline" in ''|*[!0-9]*) return 1 ;; esac
            _current="\$(date +%s)"
            [ "\$_current" -lt "\$_deadline" ] || return 1
            printf '%s' "\$(( _deadline - _current ))"
        }
        _dispatch_sweep_has_time() {
            _dispatch_sweep_remaining >/dev/null
        }
        _dispatch_sweep_timeout() {
            _remaining="\$(_dispatch_sweep_remaining)" || return 124
            if command -v timeout >/dev/null 2>&1; then
                timeout "\$_remaining" "\$@"
            else
                # The VM normally has coreutils timeout. If a minimal image
                # lacks it, keep the sweep fail-open rather than refusing the
                # dispatch; loop/deletion checks still stop future work.
                "\$@"
            fi
        }
        _dispatch_sweep_now_ms() {
            _now_ms=\$(( \$(date +%s) * 1000 ))
            printf '%s' "\$_now_ms"
        }
        _dispatch_sandbox_newest_mtime() {
            _dsn_dir="\${1:-}"
            [ -d "\$_dsn_dir" ] || return 1
            _dsn_values=
            _dsn_rc=0
            _dsn_values=\$(_dispatch_sweep_timeout find "\$_dsn_dir" ! -type l -exec stat -c %Y {} + 2>/dev/null) || _dsn_rc=\$?
            if [ "\$_dsn_rc" -ne 0 ] && _dispatch_sweep_has_time; then
                _dsn_rc=0
                _dsn_values=\$(_dispatch_sweep_timeout find "\$_dsn_dir" ! -type l -exec stat -f %m {} + 2>/dev/null) || _dsn_rc=\$?
            fi
            [ "\$_dsn_rc" -eq 0 ] || return 1
            _mt=\$(printf '%s\n' "\$_dsn_values" | awk '
                BEGIN { max = -1; count = 0; invalid = 0 }
                NF == 0 { next }
                NF != 1 || \$1 !~ /^[0-9]+$/ { invalid = 1; next }
                { count++; if (\$1 > max) max = \$1 }
                END { if (invalid || count == 0) exit 1; print max }
            ') || return 1
            return 0
        }
        _lane_sweep_unit_live() {
            _luk_key="\${1:-}"
            _lane_sweep_unit_probe_failed=0
            [ -n "\$_luk_key" ] || return 1
            if ! command -v systemctl >/dev/null 2>&1; then
                _lane_sweep_unit_probe_failed=1
                return 0
            fi
            _luk_runtime_dir="\${XDG_RUNTIME_DIR:-}"
            if [ -z "\$_luk_runtime_dir" ]; then
                _luk_uid=\$(id -u 2>/dev/null) || {
                    _lane_sweep_unit_probe_failed=1
                    return 0
                }
                case "\$_luk_uid" in ''|*[!0-9]*)
                    _lane_sweep_unit_probe_failed=1
                    return 0
                    ;;
                esac
                _luk_runtime_dir="/run/user/\$_luk_uid"
            fi
            _luk_timeout=2
            for _luk_prefix in workbay-lane grok-lane; do
                _luk_n=0
                while [ "\$_luk_n" -le 9 ]; do
                    if [ "\$_luk_n" -eq 0 ]; then
                        set -- "\${_luk_prefix}-\${_luk_key}.scope" "\${_luk_prefix}-\${_luk_key}-sv.scope"
                    else
                        set -- "\${_luk_prefix}-\${_luk_key}-r\${_luk_n}.scope" "\${_luk_prefix}-\${_luk_key}-r\${_luk_n}-sv.scope"
                    fi
                    for _luk_unit in "\$@"; do
                    if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ] && ! _dispatch_sweep_has_time; then
                        _sweep_budget_exhausted=1
                        _lane_sweep_unit_probe_failed=1
                        return 0
                    fi
                    _luk_rc=0
                    _luk_state=\$(XDG_RUNTIME_DIR="\$_luk_runtime_dir" _dispatch_sweep_timeout timeout "\$_luk_timeout" systemctl --user is-active "\$_luk_unit" 2>/dev/null) || _luk_rc=\$?
                    case "\$_luk_rc:\$_luk_state" in
                        0:active|0:activating|0:reloading|0:deactivating) return 0 ;;
                        3:inactive|3:failed|3:unknown|4:inactive|4:failed|4:unknown) ;;
                        *) _lane_sweep_unit_probe_failed=1; return 0 ;;
                    esac
                    done
                    _luk_n=\$((_luk_n + 1))
                done
            done
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ] && ! _dispatch_sweep_has_time; then
                _sweep_budget_exhausted=1
                _lane_sweep_unit_probe_failed=1
                return 0
            fi
            _luk_rc=0
            _luk_jobs=\$(XDG_RUNTIME_DIR="\$_luk_runtime_dir" _dispatch_sweep_timeout timeout "\$_luk_timeout" systemctl --user list-units --plain --no-legend "workbay-lane-\${_luk_key}-job-*" "grok-lane-\${_luk_key}-job-*" 2>/dev/null) || _luk_rc=\$?
            if [ "\$_luk_rc" -ne 0 ]; then
                _lane_sweep_unit_probe_failed=1
                return 0
            fi
            while IFS= read -r _luk_line || [ -n "\$_luk_line" ]; do
                [ -n "\$_luk_line" ] || continue
                set -- \$_luk_line
                case "\${1:-}" in
                    workbay-lane-\${_luk_key}-job-*|grok-lane-\${_luk_key}-job-*) ;;
                    *) _lane_sweep_unit_probe_failed=1; return 0 ;;
                esac
                case "\${3:-}" in
                    active|activating|reloading|deactivating) return 0 ;;
                    *) _lane_sweep_unit_probe_failed=1; return 0 ;;
                esac
            done <<< "\$_luk_jobs"
            return 1
        }
        _now=\$(date +%s)
        # Bake own nonce at local dispatch (unquoted heredoc expands it into the
        # single-quoted literal). Remote never needs DISPATCH_NONCE set under -u.
        # Extract-only harnesses that leave the token unsubstituted get a no-op
        # self-exclusion (literal '\${DISPATCH_NONCE}' matches no real path).
        _self_nonce='${DISPATCH_NONCE}'
        # Per-lane dispatch-reaper lock [CON-11]: fd 9 belongs to this dispatch
        # and fd 8 belongs to the sweep owner. fd 7 stays locked from candidate
        # liveness checks through deletion. Read-only open preserves the rule
        # that a missing lock file is not created merely to probe it.
        _lane_lock_acquire() {
            _lka_key="\${1:-}"
            _LANE_LOCK_FD_OPEN=0
            _LANE_LOCK_CUSTODY=absent
            [ -n "\$_lka_key" ] || return 1
            _lka_path="\$ROOT/.lane-lock-\$_lka_key"
            # A native global owner must also acquire the key itself, in
            # that order. Never inherit a shell key acquired before it.
            if [ "\${_sweep_custody_global:-}" = native ]; then
                _LANE_LOCK_CUSTODY=native
                return 0
            fi
            [ -f "\$_lka_path" ] || return 0
            if ! exec 7<"\$_lka_path"; then
                return 1
            fi
            if ! flock -n 7; then
                exec 7<&-
                return 1
            fi
            _LANE_LOCK_FD_OPEN=1
            _LANE_LOCK_CUSTODY=7
            return 0
        }
        _lane_lock_release() {
            if [ "\${_LANE_LOCK_FD_OPEN:-0}" -eq 1 ]; then
                exec 7<&-
            fi
            _LANE_LOCK_FD_OPEN=0
            _LANE_LOCK_CUSTODY=absent
        }
        # Shared TTL journal [OBS-08][CARD-07]: every deletion arm writes a
        # keyed started row before the unlink and reaped/incomplete after.
        # Shape matches the sandbox-TTL writer (key/outcome/reason/age/actor/at).
        # A failed unlink is incomplete, never reaped. Journal-write failure
        # on the started row refuses the delete so silence cannot look like success.
        _ttl_outcome() {
            printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                "\$1" "\$2" "\$3" "\$4" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl"
        }
        # Completion journal [HARM1FIX03RV-001][OBS-08][REF-26]: reaped only when
        # the delete returned 0 AND the absence probe ran and proved absence
        # (probe_rc=1). File probes use [ -e ]; refs use show-ref --verify --quiet
        # (0=present, 1=absent, anything else=probe failed). A nonzero rm that
        # still unlinked, or a failed instrument, is incomplete. Optional \$6 is
        # bytes_freed on reaped rows; optional \$7 records the applied TTL.
        # Return 0 iff reaped.
        _journal_deletion_outcome() {
            _jdo_key=\${1:-}
            _jdo_del=\${2:-1}
            _jdo_probe=\${3:-2}
            _jdo_reason=\${4:-}
            _jdo_age=\${5:-0}
            _jdo_bytes=\${6:-}
            _jdo_ttl=\${7:-}
            _jdo_ttl_field=
            case "\$_jdo_ttl" in
                ''|*[!0-9]*) ;;
                *) _jdo_ttl_field=",\"ttl_sec\":\$_jdo_ttl" ;;
            esac
            if [ "\$_jdo_del" -eq 0 ] && [ "\$_jdo_probe" -eq 1 ]; then
                if [ -n "\$_jdo_bytes" ]; then
                    printf '{"key":"%s","outcome":"reaped","reason":"%s","age":%s,"actor":"dispatch_ttl","bytes_freed":%s,"at":%s%s}\n' \
                        "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$_jdo_bytes" "\$(date +%s)" "\$_jdo_ttl_field" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: ttl reap completion journal failed for \$_jdo_key" >&2
                else
                    printf '{"key":"%s","outcome":"reaped","reason":"%s","age":%s,"actor":"dispatch_ttl","at":%s%s}\n' \
                        "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$(date +%s)" "\$_jdo_ttl_field" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: ttl reap completion journal failed for \$_jdo_key" >&2
                fi
                return 0
            fi
            if [ "\$_jdo_del" -ne 0 ]; then
                _jdo_step=delete
                _jdo_status=\$_jdo_del
            else
                _jdo_step=probe
                _jdo_status=\$_jdo_probe
            fi
            printf '{"key":"%s","outcome":"incomplete","reason":"%s","age":%s,"actor":"dispatch_ttl","step":"%s","status":%s,"at":%s%s}\n' \
                "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$_jdo_step" "\$_jdo_status" "\$(date +%s)" "\$_jdo_ttl_field" >>"\$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: ttl incomplete reap journal failed for \$_jdo_key" >&2
            return 1
        }
        for _p in "\$ROOT"/.lane-out-* "\$ROOT"/.brief-*.md "\$ROOT"/.schema-*.json; do
            [ -e "\$_p" ] || continue
            # Never reap THIS dispatch's own staged inputs (reachable at short TTL).
            if [ -n "\$_self_nonce" ]; then
                case "\$_p" in
                    *"\$_self_nonce"*) continue ;;
                esac
            fi
            _reap_mtime "\$_p" || continue
            [ "\$((_now - _mt))" -gt ${DISPATCH_TTL_SEC} ] || continue
            # Outbox/brief/schema: parent mtime can stay stale (esp. outbox while
            # children append logs). Occupancy gate per class [REF-10] so a
            # different-branch reaper cannot delete a LIVE lane's staged
            # brief/schema under a short operator TTL. Unparseable key or
            # occupied lease (unexpired/malformed) → skip. Age TTL remains the
            # guard for the brief window between lock win and lease write.
            case "\$_p" in
                */.lane-out-*|.lane-out-*|*/.brief-*|.brief-*|*/.schema-*|.schema-*)
                    _on=\${_p##*/}
                    _on=\${_on#.lane-out-}
                    _on=\${_on#.brief-}
                    _on=\${_on#.schema-}
                    _on=\${_on%.md}
                    _on=\${_on%.json}
                    _olk=
                    # LANE_KEY always ends with -<8hex> (branch hash); nonce follows.
                    if [[ "\$_on" =~ ^(.*-[0-9a-f]{8})- ]]; then
                        _olk="\${BASH_REMATCH[1]}"
                    fi
                    if [ -z "\$_olk" ] || _lane_occupant_live "\$_olk" || _lane_lock_held "\$_olk"; then
                        continue
                    fi
                    # Hold the lane lock from this liveness check through unlink.
                    if ! _lane_lock_acquire "\$_olk"; then
                        continue
                    fi
                    if _lane_occupant_live "\$_olk"; then
                        _lane_lock_release
                        continue
                    fi
                    ;;
            esac
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started dispatch_transient "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                _lane_lock_release
                continue
            fi
            _del_rc=0
            rm -rf "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" dispatch_transient "\$_age" || true
            _lane_lock_release
        done
        # Lane-aware warm-snapshot retention [SALVAGE-SNAPSHOT-RETENTION-003]:
        # snapshots hold discarded dirty bytes, so retain a bounded newest set
        # per lane and only reclaim entries past the dispatch TTL. Ownership,
        # live leases, and held locks are proof obligations; absence of proof
        # is a skip [RLSE-08][CON-06]. The walk and delete caps keep a damaged
        # root from turning one dispatch into an unbounded maintenance job.
        _SALVAGE_SNAPSHOT_CAP=8
        _SALVAGE_SNAPSHOT_EXAM_CAP=128
        _SALVAGE_SNAPSHOT_RM_CAP=64
        _salvage_snapshot_examined=0
        _salvage_snapshot_reclaimed=0
        # Indexed counters, not associative arrays. This body normally runs on
        # the Linux VM, but test_remote_agent_dispatch_reaper.py stubs ssh with
        # exec bash -s and executes it under the HOST bash -- 3.2.57 on macOS,
        # which has no associative arrays. Indexed arrays work on both; the
        # 128-entry examination cap bounds the linear lane lookup.
        _salvage_lane_keys=()
        _salvage_lane_counts=()
        _salvage_snapshot_old_ifs="\$IFS"
        IFS=\$'\n'
        for _p in \$(ls -1t "\$ROOT"/.salvage-*.patch 2>/dev/null || true); do
            if [ "\$_salvage_snapshot_examined" -ge "\$_SALVAGE_SNAPSHOT_EXAM_CAP" ] || \
               [ "\$_salvage_snapshot_reclaimed" -ge "\$_SALVAGE_SNAPSHOT_RM_CAP" ]; then
                break
            fi
            [ -e "\$_p" ] || continue
            _salvage_snapshot_examined=\$((_salvage_snapshot_examined + 1))
            # The file name is the ownership boundary: no symlinks, only files
            # owned by this account, and only the nonce-shaped salvage pattern.
            [ -f "\$_p" ] || continue
            [ ! -L "\$_p" ] || continue
            [ -O "\$_p" ] || continue
            _salvage_snapshot_name=\${_p##*/}
            if [[ "\$_salvage_snapshot_name" =~ ^\.salvage-(.*-[0-9a-f]{8})-[A-Za-z0-9]+\.patch\$ ]]; then
                _salvage_snapshot_lane="\${BASH_REMATCH[1]}"
            else
                continue
            fi
            _reap_mtime "\$_p" || continue
            [ "\$((_now - _mt))" -gt ${DISPATCH_TTL_SEC} ] || continue
            # Live work wins over age and count. Recheck occupancy before a
            # possible delete; the lane lock is acquired after the count gate.
            if _lane_occupant_live "\$_salvage_snapshot_lane" || _lane_lock_held "\$_salvage_snapshot_lane"; then
                continue
            fi
            _salvage_lane_index=-1
            for _salvage_lane_candidate_index in "\${!_salvage_lane_keys[@]}"; do
                if [ "\${_salvage_lane_keys[\$_salvage_lane_candidate_index]}" = "\$_salvage_snapshot_lane" ]; then
                    _salvage_lane_index="\$_salvage_lane_candidate_index"
                    break
                fi
            done
            if [ "\$_salvage_lane_index" -lt 0 ]; then
                _salvage_lane_index="\${#_salvage_lane_keys[@]}"
                _salvage_lane_keys[\$_salvage_lane_index]="\$_salvage_snapshot_lane"
                _salvage_lane_counts[\$_salvage_lane_index]=0
            fi
            _salvage_lane_count="\${_salvage_lane_counts[\$_salvage_lane_index]}"
            if [ "\$_salvage_lane_count" -lt "\$_SALVAGE_SNAPSHOT_CAP" ]; then
                _salvage_lane_counts[\$_salvage_lane_index]=\$((_salvage_lane_count + 1))
                continue
            fi
            if ! _lane_lock_acquire "\$_salvage_snapshot_lane"; then
                continue
            fi
            if _lane_occupant_live "\$_salvage_snapshot_lane"; then
                _lane_lock_release
                continue
            fi
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started salvage_snapshot "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                _lane_lock_release
                continue
            fi
            _del_rc=0
            rm -f "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            if _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" salvage_snapshot "\$_age"; then
                _salvage_snapshot_reclaimed=\$((_salvage_snapshot_reclaimed + 1))
            fi
            _lane_lock_release
        done
        IFS="\$_salvage_snapshot_old_ifs"
        printf 'remote_agent: salvage snapshot reclaimer examined=%s reclaimed=%s\n' \
            "\$_salvage_snapshot_examined" "\$_salvage_snapshot_reclaimed" >&2
        # Agent-spec sidecar reclaimer [RES-07]: the EXIT trap spares this
        # dispatch's nonce when the lane is live, deferring .agent-spec-* to
        # this age-based backstop. The spare rule is unchanged; this claims
        # the deferred class. Live set is recomputed per candidate at delete
        # time (a concurrent operator session's lanes are live on this host).
        # Fail-open: a probe error skips that file; it never aborts the pass.
        # Bound the walk so a multi-thousand-file host cannot stall a dispatch.
        _AS_EXAM_CAP=128
        _AS_RM_CAP=64
        _as_exam=0
        _as_rm=0
        _as_skip=0
        _as_live=0
        _as_self=0
        _as_age=0
        _as_lock=0
        _as_probe=0
        for _p in "\$ROOT"/.agent-spec-*.json "\$ROOT"/.agent-spec-*.argv; do
            if [ "\$_as_exam" -ge "\$_AS_EXAM_CAP" ] || [ "\$_as_rm" -ge "\$_AS_RM_CAP" ]; then
                break
            fi
            [ -e "\$_p" ] || continue
            _as_exam=\$((_as_exam + 1))
            # Finding 6432: exclude this dispatch explicitly; do not rely on timing.
            if [ -n "\$_self_nonce" ]; then
                case "\$_p" in
                    *"\$_self_nonce"*)
                        _as_skip=\$((_as_skip + 1))
                        _as_self=\$((_as_self + 1))
                        continue
                        ;;
                esac
            fi
            _reap_mtime "\$_p" || {
                _as_skip=\$((_as_skip + 1))
                _as_probe=\$((_as_probe + 1))
                continue
            }
            if [ "\$((_now - _mt))" -le ${DISPATCH_TTL_SEC} ]; then
                _as_skip=\$((_as_skip + 1))
                _as_age=\$((_as_age + 1))
                continue
            fi
            _on=\${_p##*/}
            _on=\${_on#.agent-spec-}
            _on=\${_on%.json}
            _on=\${_on%.argv}
            _olk=
            if [[ "\$_on" =~ ^(.*-[0-9a-f]{8})- ]]; then
                _olk="\${BASH_REMATCH[1]}"
            fi
            # Unparseable owner: absence of proof is not proof of absence.
            if [ -z "\$_olk" ]; then
                _as_skip=\$((_as_skip + 1))
                _as_probe=\$((_as_probe + 1))
                continue
            fi
            # Recompute occupancy on this host now, not from a pre-round-trip set.
            if _lane_occupant_live "\$_olk" || _lane_lock_held "\$_olk"; then
                _as_skip=\$((_as_skip + 1))
                _as_live=\$((_as_live + 1))
                continue
            fi
            if ! _lane_lock_acquire "\$_olk"; then
                _as_skip=\$((_as_skip + 1))
                _as_lock=\$((_as_lock + 1))
                continue
            fi
            if _lane_occupant_live "\$_olk"; then
                _lane_lock_release
                _as_skip=\$((_as_skip + 1))
                _as_live=\$((_as_live + 1))
                continue
            fi
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started agent_spec "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                _lane_lock_release
                continue
            fi
            _del_rc=0
            rm -f "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            if _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" agent_spec "\$_age"; then
                _as_rm=\$((_as_rm + 1))
            fi
            _lane_lock_release
        done
        printf 'remote_agent: agent-spec reclaimer examined=%s reclaimed=%s skipped=%s live=%s self=%s age=%s lock=%s probe=%s\n' \
            "\$_as_exam" "\$_as_rm" "\$_as_skip" "\$_as_live" "\$_as_self" "\$_as_age" "\$_as_lock" "\$_as_probe" >&2
        # Packed + loose heads via for-each-ref. The full nonce tail
        # (-<8hex>-<pid>-<16hex>) is always eligible: unambiguous by construction.
        # The bare legacy lane-key tail (-<8hex>) is name-ambiguous with real
        # branches (release-20260726, hotfix-deadbeef, … — an 8-digit date is 8
        # valid hex digits; LANE_KEY's tr sanitization is lossy so the hash
        # cannot re-verify the name). That shape is therefore opt-in via
        # REAP_LEGACY_REFS (default off). When it is on, the preserve list
        # (main/master/HEAD + KEEP_REFS) is the operator escape hatch; the
        # absolute refusal to delete the mirror's checked-out branch, the
        # live-lane occupancy gate, and self-exclusion of this dispatch's
        # nonce and bare LANE_KEY always apply. Age from reflog mtime
        # (survives pack-refs); skip when no age source. Capture status: a
        # failed for-each-ref used to yield an empty stream and silently
        # no-op the whole ref sweep — warn once, still non-fatal [AGT-10].
        _reap_refs_warned=
        _ref_list=\$(git -C "\$SRC" for-each-ref --format='%(refname:short)' refs/heads/ 2>/dev/null) || {
            if [ -z "\${_reap_refs_warned:-}" ]; then
                echo 'remote_agent: dispatch reaper cannot list refs (git for-each-ref failed) — ref sweep degraded' >&2
                _reap_refs_warned=1
            fi
            _ref_list=
        }
        # Non-bare mirrors: never delete HEAD's branch (leaves worktree broken).
        _co=\$(git -C "\$SRC" symbolic-ref --short HEAD 2>/dev/null || true)
        # KEEP_REFS / REAP_LEGACY_REFS are baked at local dispatch into
        # single-quoted literals (same pattern as _self_nonce) so
        # extract-only harnesses that leave the token unsubstituted do not
        # trip set -u.
        _keep_refs='${KEEP_REFS}'
        _reap_legacy='${REAP_LEGACY_REFS}'
        while IFS= read -r _bn; do
            [ -n "\$_bn" ] || continue
            if [ -n "\$_self_nonce" ]; then
                case "\$_bn" in
                    *"\$_self_nonce"*) continue ;;
                esac
            fi
            # Self-exclusion: bare LANE_KEY (legacy same-key ref for this dispatch).
            if [ "\$_bn" = '${LANE_KEY}' ]; then
                continue
            fi
            # Shape: full nonce tail always; bare -<8hex> only when opted in.
            if [[ ! "\$_bn" =~ -[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}\$ ]]; then
                if [ "\$_reap_legacy" != "1" ]; then
                    continue
                fi
                if [[ ! "\$_bn" =~ -[0-9a-f]{8}\$ ]]; then
                    continue
                fi
            fi
            # Never delete the mirror's checked-out branch (empty = detached HEAD).
            if [ -n "\$_co" ]; then
                if [ "\$_bn" = "\$_co" ]; then
                    continue
                fi
            fi
            # Preserve list: built-in main/master/HEAD + operator KEEP_REFS.
            _keep=0
            for _kr in main master HEAD \$_keep_refs; do
                if [ "\$_bn" = "\$_kr" ]; then
                    _keep=1
                    break
                fi
            done
            if [ "\$_keep" -eq 1 ]; then
                continue
            fi
            # Live-lane gate: nonce'd ref → LANE_KEY is the -<8hex> prefix;
            # legacy ref → whole name is the lane key.
            _olk=
            if [[ "\$_bn" =~ ^(.*-[0-9a-f]{8})-[0-9]+-[0-9a-f]{16}\$ ]]; then
                _olk="\${BASH_REMATCH[1]}"
            elif [[ "\$_bn" =~ -[0-9a-f]{8}\$ ]]; then
                _olk="\$_bn"
            fi
            if [ -z "\$_olk" ] || _lane_occupant_live "\$_olk" || _lane_lock_held "\$_olk"; then
                continue
            fi
            _rl="\$SRC/.git/logs/refs/heads/\$_bn"
            [ -f "\$_rl" ] || continue
            _reap_mtime "\$_rl" || continue
            [ "\$((_now - _mt))" -gt ${DISPATCH_TTL_SEC} ] || continue
            if ! _lane_lock_acquire "\$_olk"; then
                continue
            fi
            if _lane_occupant_live "\$_olk"; then
                _lane_lock_release
                continue
            fi
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_bn" started dispatch_ref "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_bn; deletion refused" >&2
                _lane_lock_release
                continue
            fi
            _del_rc=0
            git -C "\$SRC" update-ref -d "refs/heads/\$_bn" 2>/dev/null || _del_rc=\$?
            git -C "\$SRC" show-ref --verify --quiet "refs/heads/\$_bn" 2>/dev/null && _probe_rc=0 || _probe_rc=\$?
            _journal_deletion_outcome "\$_bn" "\$_del_rc" "\$_probe_rc" dispatch_ref "\$_age" || true
            _lane_lock_release
        done <<< "\$_ref_list"
        # Per-lane sandbox reaper [RES-07]: marker-gated age TTL for \$ROOT/<LANE_KEY>
        # worktrees and their .venv-lane-* / .venv-sync-stamp-* siblings. Only
        # directories this script marked are candidates — never a name heuristic
        # (archive-20260719 ends in 8 hex-valid chars). Own \$SBX and live leases
        # are excluded. 0 disables only this sweep; DISPATCH_TTL_SEC=0 is the
        # master off switch for the whole reaper block.
        if [ '${SANDBOX_TTL_SEC}' -gt 0 ] 2>/dev/null; then
            (
            # Reclaim counters [OBS-08]: track deletions only (not candidates).
            # Report one stderr line when either is non-zero; quiet passes stay quiet.
            _reclaimed_sandboxes=0
            _reclaimed_orphan_venvs=0
            _sweep_reaped=0
            _sweep_freed_kb=0
            _sweep_candidates=0
            _sweep_unit_probe_failed=0
            _sweep_budget_exhausted=0
            _sweep_first_unclassified_pid=
            _sweep_first_unclassified_comm=
            _sweep_first_unclassified_uid=
            _sweep_started=\$(date +%s)
            _sweep_started_ms=\$(_dispatch_sweep_now_ms)
            _sweep_interval='${SWEEP_MIN_INTERVAL_SEC}'
            case "\$_sweep_interval" in ''|*[!0-9]*) _sweep_interval=60 ;; esac
            _sweep_budget='${SANDBOX_REAP_BUDGET_SEC}'
            case "\$_sweep_budget" in ''|*[!0-9]*) _sweep_budget=60 ;; esac
            # A zero budget is still a bounded one-second opportunity. Under
            # pressure, reclaim is the recovery path: double the configured
            # window so one eligible candidate is not lost to a too-small default.
            [ "\$_sweep_budget" -gt 0 ] || _sweep_budget=1
            _sweep_used_pct=\$(df -P "\$ROOT" 2>/dev/null | awk 'NR==2 { gsub(/%/,"",\$5); print \$5+0 }')
            case "\$_sweep_used_pct" in ''|*[!0-9]*) _sweep_used_pct=0 ;; esac
            _sweep_pressure_pct="\${WORKBAY_REMOTE_AGENT_DISK_PRESSURE_PCT:-85}"
            case "\$_sweep_pressure_pct" in ''|*[!0-9]*) _sweep_pressure_pct=85 ;; esac
            _sweep_pressured=0
            if [ "\$_sweep_used_pct" -ge "\$_sweep_pressure_pct" ]; then
                _sweep_pressured=1
                _sweep_budget=\$(( _sweep_budget * 2 ))
            fi
            _sweep_ttl='${SANDBOX_TTL_SEC}'
            _sweep_idle='${SANDBOX_IDLE_SEC}'
            case "\$_sweep_idle" in ''|*[!0-9]*) _sweep_idle=21600 ;; esac
            if [ "\$_sweep_pressured" -eq 1 ]; then
                _sweep_ttl_cap="\$_sweep_idle"
                if [ "\$_sweep_used_pct" -ge 95 ]; then
                    _sweep_ttl_cap=7200
                    if [ "\$_sweep_idle" -lt "\$_sweep_ttl_cap" ]; then
                        _sweep_ttl_cap="\$_sweep_idle"
                    fi
                fi
                if [ "\$_sweep_ttl" -gt "\$_sweep_ttl_cap" ]; then
                    _sweep_ttl="\$_sweep_ttl_cap"
                fi
            fi
            _sweep_stamp="\$ROOT/.reap-last.json"
            _sweep_stamp_at=
            _sweep_run=1
            _sweep_skip_reason=
            _sweep_owner_lock=unavailable
            if [ -s "\$_sweep_stamp" ]; then
                _sweep_stamp_json=\$(sed -n '1p' "\$_sweep_stamp" 2>/dev/null || true)
                case "\$_sweep_stamp_json" in
                    \{*\})
                        _sweep_stamp_at=\$(sed -n 's/.*"at"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "\$_sweep_stamp" 2>/dev/null | head -n1 || true)
                        ;;
                esac
            fi
            _dispatch_sweep_skip_outcome() {
                printf '{"key":"${LANE_KEY}","outcome":"skipped","reason":"%s","actor":"dispatch_ttl","ttl_sec":%s,"at":%s}\n' \
                    "\$_sweep_skip_reason" "\$_sweep_ttl" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: sandbox sweep skip journal failed for ${LANE_KEY}" >&2
            }
            _dispatch_sweep_capture_proc_unknowns() {
                local _capture_pid _capture_uid _capture_comm _capture_reason
                _sweep_unit_probe_failed=0
                _sweep_first_unclassified_pid=
                _sweep_first_unclassified_comm=
                _sweep_first_unclassified_uid=
                while IFS=\$'\t' read -r _capture_pid _capture_uid _capture_comm _capture_reason || [ -n "\${_capture_pid:-}" ]; do
                    [ -n "\$_capture_pid" ] || continue
                    _sweep_unit_probe_failed=\$(( _sweep_unit_probe_failed + 1 ))
                    if [ -z "\$_sweep_first_unclassified_pid" ]; then
                        _sweep_first_unclassified_pid="\$_capture_pid"
                        _sweep_first_unclassified_comm="\$_capture_comm"
                        _sweep_first_unclassified_uid="\$_capture_uid"
                    fi
                done <<< "\${_PROC_CWD_UNCLASSIFIED_ROWS:-}"
            }
            _dispatch_sweep_write_receipt() {
                _sweep_finished_ms=\$(_dispatch_sweep_now_ms)
                _sweep_elapsed_ms=\$(( _sweep_finished_ms - _sweep_started_ms ))
                [ "\$_sweep_elapsed_ms" -ge 0 ] || _sweep_elapsed_ms=0
                if [ "\$_sweep_budget_exhausted" -eq 1 ] && { [ "\$_sweep_candidates" -le 0 ] || [ "\$_sweep_elapsed_ms" -lt \$(( _sweep_budget * 1000 )) ]; }; then
                    _sweep_budget_exhausted=0
                    if [ -z "\$_sweep_skip_reason" ] || [ "\$_sweep_skip_reason" = budget_exhausted ]; then
                        _sweep_skip_reason=budget_deferred_before_candidate
                    fi
                fi
                if [ "\$_sweep_budget_exhausted" -eq 1 ] && [ -z "\$_sweep_skip_reason" ]; then
                    _sweep_skip_reason=budget_exhausted
                fi
                _sweep_done_at="\$_sweep_stamp_at"
                case "\$_sweep_done_at" in ''|*[!0-9]*) _sweep_done_at="\$_sweep_started" ;; esac
                if [ -z "\$_sweep_skip_reason" ] && [ "\$_sweep_budget_exhausted" -eq 0 ]; then
                    _sweep_done_at=\$(date +%s)
                fi
                if [ "\$_sweep_pressured" -eq 1 ]; then
                    _sweep_pressured_json=true
                else
                    _sweep_pressured_json=false
                fi
                if [ "\$_sweep_budget_exhausted" -eq 1 ]; then
                    _sweep_budget_exhausted_json=true
                else
                    _sweep_budget_exhausted_json=false
                fi
                _sweep_first_unclassified_json=null
                if [ -n "\$_sweep_first_unclassified_pid" ]; then
                    case "\$_sweep_first_unclassified_pid" in ''|*[!0-9]*) _first_pid_json=null ;; *) _first_pid_json="\$_sweep_first_unclassified_pid" ;; esac
                    case "\$_sweep_first_unclassified_uid" in ''|*[!0-9]*) _first_uid_json=null ;; *) _first_uid_json="\$_sweep_first_unclassified_uid" ;; esac
                    _sweep_first_unclassified_json="{\"pid\":\$_first_pid_json,\"comm\":\"\$_sweep_first_unclassified_comm\",\"uid\":\$_first_uid_json}"
                fi
                if ! printf '{"at":%s,"pressured":%s,"candidates":%s,"swept":%s,"reaped":%s,"freed_kb":%s,"actor":"dispatch_ttl","elapsed_ms":%s,"skip_reason":"%s","budget_exhausted":%s,"ttl_sec":%s,"unit_probe_failed":%s,"first_unclassified":%s,"owner_lock":"%s"}\n' \
                    "\$_sweep_done_at" "\$_sweep_pressured_json" "\$_sweep_candidates" "\$_sweep_candidates" "\$_sweep_reaped" "\$_sweep_freed_kb" \
                    "\$_sweep_elapsed_ms" "\$_sweep_skip_reason" "\$_sweep_budget_exhausted_json" "\$_sweep_ttl" "\$_sweep_unit_probe_failed" "\$_sweep_first_unclassified_json" "\$_sweep_owner_lock" \
                    >"\$_sweep_stamp.tmp" 2>/dev/null || ! mv -f "\$_sweep_stamp.tmp" "\$_sweep_stamp" 2>/dev/null; then
                    echo 'remote_agent: cannot persist dispatch sandbox sweep stamp — sweep remains fail-open' >&2
                    rm -f "\$_sweep_stamp.tmp" 2>/dev/null || true
                fi
            }
            _dispatch_sweep_journal_candidate_probe() {
                local _journal_key="\${1:-}" _journal_outcome="\${2:-}" _journal_reason="\${3:-}" _journal_age="\${4:-0}" _journal_ttl="\${5:-0}"
                if [ "\$_journal_outcome" = kept_proc_probe_unknown ]; then
                    local _journal_pid="\${_PROC_CWD_MATCHED_PID:-\${_PROC_CWD_FAILURE_PID:-unknown}}"
                    local _journal_comm="\${_PROC_CWD_MATCHED_COMM:-\${_PROC_CWD_FAILURE_COMM:-unknown}}"
                    local _journal_uid="\${_PROC_CWD_MATCHED_UID:-\${_PROC_CWD_FAILURE_UID:-unknown}}"
                    case "\$_journal_pid" in ''|*[!0-9]*) _journal_pid_json=null ;; *) _journal_pid_json="\$_journal_pid" ;; esac
                    case "\$_journal_uid" in ''|*[!0-9]*) _journal_uid_json=null ;; *) _journal_uid_json="\$_journal_uid" ;; esac
                    printf '{"key":"%s","outcome":"%s","reason":"%s","pid":%s,"comm":"%s","uid":%s,"age":%s,"actor":"dispatch_ttl","ttl_sec":%s,"at":%s}\n' \
                        "\$_journal_key" "\$_journal_outcome" "\$_journal_reason" "\$_journal_pid_json" "\$_journal_comm" "\$_journal_uid_json" \
                        "\$_journal_age" "\$_journal_ttl" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: kept_proc_probe_unknown journal failed for \$_journal_key" >&2
                else
                    printf '{"key":"%s","outcome":"%s","reason":"%s","age":%s,"actor":"dispatch_ttl","ttl_sec":%s,"at":%s}\n' \
                        "\$_journal_key" "\$_journal_outcome" "\$_journal_reason" "\$_journal_age" "\$_journal_ttl" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: \$_journal_outcome journal failed for \$_journal_key" >&2
                fi
            }
            _sweep_custody_global=8
            _sweep_owner_rc=0
            if [ "\${_lane_flock_available:-1}" -eq 0 ]; then
                # Leave creation/open to root-relative O_NOFOLLOW native
                # custody; a shell redirection is not a safe fallback fence.
                _sweep_owner_rc=127
            elif [ -L "\$ROOT/.reap-sweep.lock" ] || \
                { [ -e "\$ROOT/.reap-sweep.lock" ] && [ ! -f "\$ROOT/.reap-sweep.lock" ]; } || \
                ! exec 8>>"\$ROOT/.reap-sweep.lock"; then
                _sweep_owner_lock=busy
                _sweep_skip_reason=sweep_owner_busy
                echo 'remote_agent: sandbox sweep skipped: sweep_owner_busy' >&2
                _dispatch_sweep_skip_outcome
                _dispatch_sweep_write_receipt
                exit 0
            else
                flock -n 8 || _sweep_owner_rc=\$?
            fi
            case "\$_sweep_owner_rc" in
                0) _sweep_owner_lock=held ;;
                126|127)
                    exec 8>&-
                    _sweep_custody_global=native
                    echo 'remote_agent: sandbox sweep owner_lock=unavailable; native custody will acquire the global fence' >&2
                    ;;
                *)
                    _sweep_owner_lock=error
                    _sweep_skip_reason=sweep_owner_error
                    if [ "\$_sweep_owner_rc" -eq 1 ]; then
                        _sweep_owner_lock=busy
                        _sweep_skip_reason=sweep_owner_busy
                    fi
                    echo "remote_agent: sandbox sweep skipped: \$_sweep_skip_reason" >&2
                    _dispatch_sweep_skip_outcome
                    _dispatch_sweep_write_receipt
                    exit 0
                    ;;
            esac
            if [ "\$_sweep_pressured" -eq 0 ] && [ "\$_sweep_interval" -gt 0 ] && [ -n "\$_sweep_stamp_at" ]; then
                case "\$_sweep_stamp_at" in
                    ''|*[!0-9]*) ;;
                    *)
                        if [ "\$_sweep_stamp_at" -le "\$_sweep_started" ] && [ "\$(( _sweep_started - _sweep_stamp_at ))" -lt "\$_sweep_interval" ]; then
                            _sweep_run=0
                            _sweep_skip_reason=interval_not_elapsed
                        fi
                        ;;
                esac
            fi
            if [ "\$_sweep_run" -eq 0 ]; then
                echo "remote_agent: sandbox sweep completed \${_sweep_stamp_at}s ago; skipping fresh pass" >&2
                _dispatch_sweep_skip_outcome
            else
                _DISPATCH_SWEEP_DEADLINE=\$(( _sweep_started + _sweep_budget ))
                _DISPATCH_SWEEP_PER_CANDIDATE=1
                _sweep_cursor_file="\$ROOT/.reap-sweep-cursor"
                _sweep_cursor=\$(sed -n '1p' "\$_sweep_cursor_file" 2>/dev/null || true)
                _sweep_keys=()
                if _snapshot_proc_cwds; then
                    _dispatch_sweep_capture_proc_unknowns
                    for _sd in "\$ROOT"/*/; do
                        _sd="\${_sd%/}"
                        printf '%s\n' "\${_sd##*/}" | LC_ALL=C grep -Eq '^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}\$' && continue
                        [ ! -f "\$_sd/.workbay-attempt-evidence" ] || continue
                        # Marker-gated: unmarked operator dirs are never candidates.
                        [ -f "\$_sd/.workbay-lane-sandbox" ] || continue
                        # Never reap this dispatch's own sandbox (or its warm venv).
                        [ "\$_sd" = "\$SBX" ] && continue
                        _sweep_keys+=("\${_sd##*/}")
                    done
                else
                    _sweep_budget_exhausted=0
                    _sweep_probe_reason="proc_probe_unknown:\${_PROC_CWD_PROBE_REASON:-proc_probe_failed}"
                    _sweep_skip_reason="\$_sweep_probe_reason"
                    _sweep_unit_probe_failed=1
                    _sweep_first_unclassified_pid="\${_PROC_CWD_FAILURE_PID:-unknown}"
                    _sweep_first_unclassified_comm="\${_PROC_CWD_FAILURE_COMM:-unknown}"
                    _sweep_first_unclassified_uid="\${_PROC_CWD_FAILURE_UID:-unknown}"
                    echo "remote_agent: dispatch sandbox occupancy probe unknown (\$_sweep_probe_reason) — sweep deferred" >&2
                    printf '{"outcome":"sweep_deferred","reason":"%s","actor":"dispatch_ttl","at":%s}\n' \
                        "\$_sweep_probe_reason" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo 'remote_agent: dispatch sandbox probe outcome journal failed' >&2
                fi
                _sweep_count=\${#_sweep_keys[@]}
                _sweep_start=0
                if [ -n "\$_sweep_cursor" ] && [ "\$_sweep_count" -gt 0 ]; then
                    _sweep_i=0
                    while [ "\$_sweep_i" -lt "\$_sweep_count" ]; do
                        if [ "\${_sweep_keys[\$_sweep_i]}" = "\$_sweep_cursor" ]; then
                            _sweep_start=\$(( _sweep_i + 1 ))
                            [ "\$_sweep_start" -lt "\$_sweep_count" ] || _sweep_start=0
                            break
                        fi
                        _sweep_i=\$(( _sweep_i + 1 ))
                    done
                fi
                _sweep_i=0
                while [ "\$_sweep_i" -lt "\$_sweep_count" ]; do
                    if ! _dispatch_sweep_has_time; then
                        _sweep_budget_exhausted=1
                        break
                    fi
                    _sweep_index=\$(( _sweep_start + _sweep_i ))
                    [ "\$_sweep_index" -lt "\$_sweep_count" ] || _sweep_index=\$(( _sweep_index - _sweep_count ))
                    _sk="\${_sweep_keys[\$_sweep_index]}"
                    _sd="\$ROOT/\$_sk"
                    _sweep_candidates=\$(( _sweep_candidates + 1 ))
                    # Persist the last visited key so a partial pass resumes
                    # after the old prefix. Lexical order plus this rotation
                    # gives the tail a turn instead of timing out on one key.
                    printf '%s\n' "\$_sk" >"\$_sweep_cursor_file.tmp" 2>/dev/null && \
                        mv -f "\$_sweep_cursor_file.tmp" "\$_sweep_cursor_file" 2>/dev/null || true
                # Hold the lane lock from liveness probes through the delete.
                if _lane_lock_held "\$_sk" || ! _lane_lock_acquire "\$_sk"; then
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                # Live-lane fail-safe: unexpired/malformed lease outranks age.
                if _lane_occupant_live "\$_sk"; then
                    _lane_lock_release
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                # When pressure shortens the TTL, age from the newest entry
                # mtime so fresh activity below the sandbox root keeps it alive.
                # The marker remains the fallback clock when the configured TTL
                # is already shorter than the pressure cap.
                if [ "\$_sweep_ttl" -lt ${SANDBOX_TTL_SEC} ]; then
                    _dispatch_sandbox_newest_mtime "\$_sd" || { _lane_lock_release; _sweep_i=\$(( _sweep_i + 1 )); continue; }
                else
                    _reap_mtime "\$_sd/.workbay-lane-sandbox" || { _lane_lock_release; _sweep_i=\$(( _sweep_i + 1 )); continue; }
                fi
                _age=\$(( \$(date +%s) - _mt ))
                    if _lane_is_merged_marked "\$_sk"; then
                        if ! _lane_stop_processes "\$_sk" "\$_sd"; then
                            _probe_outcome=kept_process_survived
                            _probe_reason=sandbox_merged
                            if [ "\${_LANE_STOP_REASON:-}" = stop_budget_exhausted ]; then
                                _probe_outcome=kept_process_cleanup_budget_exhausted
                                _probe_reason=stop_budget_exhausted
                            elif [ "\${_PROC_CWD_PROBE_REASON:-}" != live_pid ] && [ -n "\${_PROC_CWD_PROBE_REASON:-}" ]; then
                                _probe_outcome=kept_proc_probe_unknown
                                _probe_reason="proc_probe_unknown:\${_PROC_CWD_PROBE_REASON}"
                            fi
                            _dispatch_sweep_journal_candidate_probe "\$_sk" "\$_probe_outcome" "\$_probe_reason" "\$_age" "\$_sweep_ttl"
                            _lane_lock_release
                            _sweep_i=\$(( _sweep_i + 1 ))
                            continue
                    fi
                    # CON-11: a pid can enter the sandbox after the pre-loop
                    # snapshot (for example during systemctl stop). Re-probe
                    # live immediately before unlink. Distinct from
                    # kept_process_survived (a snapshot pid that outlived
                    # TERM/KILL).
                    if _pid_in_sandbox_live "\$_sd"; then
                        _probe_outcome=kept_late_pid
                        _probe_reason=sandbox_merged
                        if [ -n "\${_PROC_CWD_PROBE_REASON:-}" ] && [ "\$_PROC_CWD_PROBE_REASON" != live_pid ]; then
                            _probe_outcome=kept_proc_probe_unknown
                            _probe_reason="proc_probe_unknown:\${_PROC_CWD_PROBE_REASON}"
                        fi
                        _dispatch_sweep_journal_candidate_probe "\$_sk" "\$_probe_outcome" "\$_probe_reason" "\$_age" "\$_sweep_ttl"
                        _lane_lock_release
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                else
                    # A detached child can outlive both shell lock and lease.
                    # TTL-only path never terminates; it reports the skip.
                    if _pid_in_sandbox "\$_sd"; then
                        _probe_outcome=kept_live_pid
                        _probe_reason=sandbox_ttl
                        if [ -n "\${_PROC_CWD_PROBE_REASON:-}" ] && [ "\$_PROC_CWD_PROBE_REASON" != live_pid ]; then
                            _probe_outcome=kept_proc_probe_unknown
                            _probe_reason="proc_probe_unknown:\${_PROC_CWD_PROBE_REASON}"
                        fi
                        _dispatch_sweep_journal_candidate_probe "\$_sk" "\$_probe_outcome" "\$_probe_reason" "\$_age" "\$_sweep_ttl"
                        _lane_lock_release
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                    [ "\$(( \$(date +%s) - _mt ))" -gt "\$_sweep_ttl" ] || { _lane_lock_release; _sweep_i=\$(( _sweep_i + 1 )); continue; }
                    if [ "\$_sweep_pressured" -eq 1 ] && _lane_sweep_unit_live "\$_sk"; then
                        if [ "\$_lane_sweep_unit_probe_failed" -eq 1 ]; then
                            _unit_outcome=kept_unit_probe_failed
                            _sweep_unit_probe_failed=\$(( _sweep_unit_probe_failed + 1 ))
                        else
                            _unit_outcome=kept_live_unit
                        fi
                        printf '{"key":"%s","outcome":"%s","reason":"sandbox_ttl","age":%s,"actor":"dispatch_ttl","ttl_sec":%s,"at":%s}\n' \
                            "\$_sk" "\$_unit_outcome" "\$_age" "\$_sweep_ttl" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                            echo "remote_agent: \$_unit_outcome journal failed for \$_sk" >&2
                        _lane_lock_release
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                    if _pid_in_sandbox_live "\$_sd"; then
                        _probe_outcome=kept_live_pid
                        _probe_reason=sandbox_ttl
                        if [ -n "\${_PROC_CWD_PROBE_REASON:-}" ] && [ "\$_PROC_CWD_PROBE_REASON" != live_pid ]; then
                            _probe_outcome=kept_proc_probe_unknown
                            _probe_reason="proc_probe_unknown:\${_PROC_CWD_PROBE_REASON}"
                        fi
                        _dispatch_sweep_journal_candidate_probe "\$_sk" "\$_probe_outcome" "\$_probe_reason" "\$_age" "\$_sweep_ttl"
                        _lane_lock_release
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                fi
                # Cache outcomes are accounted separately by the custody guard.
                _before=\$(du -sb "\$_sd" 2>/dev/null | awk '{sum += \$1} END {print sum + 0}' || true)
                case "\$_before" in ''|*[!0-9]*) _before=0 ;; esac
                if ! printf '{"key":"%s","outcome":"started","reason":"sandbox_ttl","age":%s,"actor":"dispatch_ttl","bytes_before":%s,"ttl_sec":%s,"at":%s}\n' \
                    "\$_sk" "\$_age" "\$_before" "\$_sweep_ttl" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl"; then
                    echo "remote_agent: cannot persist ttl reap intent for \$_sk; deletion refused" >&2
                    _lane_lock_release
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                # The source-CWD census alone does not prove its sibling cache
                # unused: FD/env/maps references and archival custody also win.
                if ! _lane_cache_reap_locked "\$_sk" sandbox_ttl_cache dispatch_ttl "\$_sweep_custody_global" "\$_LANE_LOCK_CUSTODY" pair; then
                    if [ ! -e "\$_sd" ]; then
                        _journal_deletion_outcome "\$_sk" 1 1 sandbox_ttl "\$_age" "\$_before" "\$_sweep_ttl" || true
                    else
                        _dispatch_sweep_journal_candidate_probe "\$_sk" kept_cache_custody cache_custody_hold_or_incomplete "\$_age" "\$_sweep_ttl"
                    fi
                    _lane_lock_release
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                _del_rc=0
                _probe_rc=0
                [ -e "\$_sd" ] || _probe_rc=\$?
                if _journal_deletion_outcome "\$_sk" "\$_del_rc" "\$_probe_rc" sandbox_ttl "\$_age" "\$_before" "\$_sweep_ttl"; then
                    _reclaimed_sandboxes=\$((_reclaimed_sandboxes + 1))
                    _sweep_reaped=\$(( _sweep_reaped + 1 ))
                    _sweep_freed_kb=\$(( _sweep_freed_kb + _before / 1024 ))
                fi
                    _lane_lock_release
                    _sweep_i=\$(( _sweep_i + 1 ))
                done
                if [ "\$_sweep_budget_exhausted" -eq 0 ]; then
                    if _dispatch_sweep_has_time; then
                        _orphans_killed=\$(_lane_reap_deleted_cwd_orphans)
                        if ! _dispatch_sweep_has_time; then
                            _sweep_budget_exhausted=1
                        fi
                    else
                        _sweep_budget_exhausted=1
                        _orphans_killed=0
                    fi
                else
                    _orphans_killed=0
                fi
            _DISPATCH_SWEEP_PER_CANDIDATE=0
            case "\$_orphans_killed" in ''|*[!0-9]*) _orphans_killed=0 ;; esac
            # Orphan lane-venv reaper [RES-07][OBS-08]: persisted venvs live
            # outside the sandbox so the per-pass wipe leaves them behind. When
            # the sandbox is already gone the marker-gated sweep never sees the
            # pair; reclaim venv+stamp only when all six hold: directory, not
            # this dispatch's key, no sandbox path, lease not live, lock not
            # held, older than SANDBOX_TTL_SEC. Half-pairs are their own leak.
            for _vd in "\$ROOT"/.venv-lane-*/; do
                if ! _dispatch_sweep_has_time; then
                    _sweep_budget_exhausted=1
                    break
                fi
                # Only directories (trailing-slash glob); skip literal no-match.
                if [ ! -d "\$_vd" ]; then
                    continue
                fi
                _vd="\${_vd%/}"
                _sk="\${_vd##*/.venv-lane-}"
                # This dispatch is about to materialize its sandbox — not an orphan.
                if [ "\$_sk" = '${LANE_KEY}' ]; then
                    continue
                fi
                # Sandbox still present → not an orphan (sandbox sweep owns it).
                if [ -e "\$ROOT/\$_sk" ]; then
                    continue
                fi
                # Hold the lane lock through its final lease check and venv deletion.
                if _lane_lock_held "\$_sk" || ! _lane_lock_acquire "\$_sk"; then
                    continue
                fi
                if _lane_occupant_live "\$_sk"; then
                    _lane_lock_release
                    continue
                fi
                if [ "\$_sweep_ttl" -lt ${SANDBOX_TTL_SEC} ]; then
                    _dispatch_sandbox_newest_mtime "\$_vd" || { _lane_lock_release; continue; }
                else
                    _reap_mtime "\$_vd" || { _lane_lock_release; continue; }
                fi
                [ "\$((\$(date +%s) - _mt))" -gt "\$_sweep_ttl" ] || { _lane_lock_release; continue; }
                _age=\$((_now - _mt))
                _before=\$(du -sb "\$_vd" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null | awk '{sum += \$1} END {print sum + 0}' || true)
                case "\$_before" in ''|*[!0-9]*) _before=0 ;; esac
                # The cache guard owns intent and terminal cache events. A
                # refused probe is held, not a failed deletion. Do not append
                # a second started/incomplete pair for an untouched cache.
                if ! _lane_cache_reap_locked "\$_sk" orphan_venv dispatch_ttl "\$_sweep_custody_global" "\$_LANE_LOCK_CUSTODY"; then
                    _lane_lock_release
                    continue
                fi
                if [ ! -e "\$_vd" ]; then
                    _reclaimed_orphan_venvs=\$((_reclaimed_orphan_venvs + 1))
                    _sweep_reaped=\$(( _sweep_reaped + 1 ))
                    _sweep_freed_kb=\$(( _sweep_freed_kb + _before / 1024 ))
                fi
                _lane_lock_release
            done
            if [ "\$_sweep_budget_exhausted" -eq 1 ]; then
                echo 'remote_agent: sandbox sweep budget exhausted; remaining candidates deferred' >&2
            else
                rm -f "\$_sweep_cursor_file" "\$_sweep_cursor_file.tmp" 2>/dev/null || true
            fi
            _dispatch_sweep_write_receipt
            fi
            # One stderr line per non-empty pass; never stdout (dispatch protocol).
            if [ "\$_reclaimed_sandboxes" -gt 0 ] || [ "\$_reclaimed_orphan_venvs" -gt 0 ]; then
                printf 'remote_agent: reaper reclaimed sandboxes=%s orphan_venvs=%s\n' \
                    "\$_reclaimed_sandboxes" "\$_reclaimed_orphan_venvs" >&2
            fi
            ) || true
        fi
        fi
        _prune_probe_trust || true
        _dispatch_reaper_guard_release || true
    } || true
fi
if [ "\$_lane_flock_available" -eq 0 ]; then
    echo 'remote_agent: flock unavailable on the VM — reaper completed; refusing lane materialization' >&2
    exit 78
fi
# Bound ladder: resolve once before occupancy re-check so arm-4 exit 7 runs
# before _lane_write_live_lease and before rm -rf of this lane's sandbox.
resolve_agent_bound
# BEGIN LANE_WIPE_GUARD
# Occupancy re-check after lock [CON-11][RES-10]: a prior same-key dispatch may
# have lost its shell (and the lock) while its agent still holds an unexpired
# lease. Never wipe a live sandbox — defer on the exit-75 contract. Malformed
# lease is OCCUPIED (fail-safe). No host probes.
if _lane_occupant_live '${LANE_KEY}'; then
    _lane_expiry=\$(sed -n 's/^expiry=//p' "\$ROOT/.lane-live-${LANE_KEY}" 2>/dev/null | head -n1 || true)
    printf 'remote_agent: lane lease expiry epoch=%s\n' "\${_lane_expiry:-unknown}" >&2
    echo 'remote_agent: same-branch lane still occupying sandbox (${LANE_KEY}) — deferring' >&2
    exit 75
fi
# Declare this dispatch's occupancy before the destructive wipe so a later
# SIGKILL'd peer that loses the lock still advertises the sandbox as live.
_lane_write_live_lease
if ! _snapshot_dirty_sandbox; then
    echo 'remote_agent: warm sandbox dirty snapshot failed — refusing to wipe' >&2
    exit 78
fi
rm -rf "\$SBX"
# END LANE_WIPE_GUARD
mkdir -p "\$SBX"
# Sandbox marker: the ONLY thing that makes a directory a reap candidate.
# Marker-gated (not name-gated) so operator dirs like archive-20260719 are safe.
# mtime of this file is the sandbox sweep's age source.
printf 'lane_key=%s\n' '${LANE_KEY}' > "\$SBX/.workbay-lane-sandbox"
# Per-dispatch outbox OUTSIDE \$SBX: a deferred lane taking the lock and wiping
# \$SBX must not destroy or expose this dispatch's artifacts mid-fetch [CON-12][OBS-08].
OUT_DIR="\$ROOT/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}"
mkdir -p "\$OUT_DIR"
# RES-17: announce the recovery address before exec, even if transport is lost.
ATTEMPT_EVIDENCE_DIR="\$ROOT/attempt-evidence-${LANE_KEY}-${DISPATCH_NONCE}"
mkdir -p "\$ATTEMPT_EVIDENCE_DIR"
echo "remote_agent: attempt_evidence dir=\$ATTEMPT_EVIDENCE_DIR nonce=${DISPATCH_NONCE}" >&2
_retain_attempt_evidence() {
    # CARD-03: a failed status read is unknown, never an empty successful listing.
    if [ "\${HARVEST_GITDIR_UNSAFE:-0}" = 1 ] ||
        ! git -c core.fsmonitor=false -C "\$SBX" status --porcelain --untracked-files=all > "\$ATTEMPT_EVIDENCE_DIR/untracked.txt.tmp" 2> "\$ATTEMPT_EVIDENCE_DIR/untracked.stderr"; then
        rm -f "\$ATTEMPT_EVIDENCE_DIR/untracked.txt.tmp"
        echo 'remote_agent: attempt evidence status unreadable' >&2
    else
        mv "\$ATTEMPT_EVIDENCE_DIR/untracked.txt.tmp" "\$ATTEMPT_EVIDENCE_DIR/untracked.txt"
    fi
    for _evidence_file in .agent-result.json .grok-result.json .agent-phases.json .grok-phases.json .workbay-source-provenance.json .agent-stream.jsonl .grok-debug.log .grok-selfverify.json uncommitted-remainder.patch; do
        if [ -f "\$OUT_DIR/\$_evidence_file" ] && [ ! -L "\$OUT_DIR/\$_evidence_file" ]; then
            if mv "\$OUT_DIR/\$_evidence_file" "\$ATTEMPT_EVIDENCE_DIR/\$_evidence_file"; then
                # Preserve the existing host fetch paths until transient cleanup.
                ln -s "\$ATTEMPT_EVIDENCE_DIR/\$_evidence_file" "\$OUT_DIR/\$_evidence_file" || true
            fi
        fi
    done
    # The typed evidence reaper owns retained evidence. Mark on exit
    # so an active attempt cannot age into a reap candidate during execution.
    printf 'lane_key=%s\n' '${LANE_KEY}' > "\$ATTEMPT_EVIDENCE_DIR/.workbay-attempt-evidence"
}
trap '_emit_phases_record || true; _retain_attempt_evidence || true; _lane_clear_live_lease' EXIT
# remote_preamble closes at archive_extract start; both use the VM clock.
_archive_start=\$(date +%s)
_phase_record remote_preamble "\$_RP_ENTRY" "\$_archive_start"
# Verify receive-pack materialized the host's immutable source pin before
# reading or extracting it. Keep both commit and tree checks: the former binds
# history identity, while the latter makes the transported content explicit.
if ! REMOTE_SOURCE_COMMIT=\$(git -C "\$SRC" rev-parse --verify 'refs/heads/${LANE_KEY}-${DISPATCH_NONCE}^{commit}' 2>/dev/null); then
    echo 'remote_agent: source_identity_failure=source_ref_missing dispatch_ref=refs/heads/${LANE_KEY}-${DISPATCH_NONCE}' >&2
    exit 12
fi
if [ "\$REMOTE_SOURCE_COMMIT" != '${SOURCE_COMMIT}' ]; then
    echo "remote_agent: source_identity_failure=source_commit_mismatch source commit mismatch: expected ${SOURCE_COMMIT}, got \$REMOTE_SOURCE_COMMIT" >&2
    exit 13
fi
REMOTE_SOURCE_TREE=\$(git -C "\$SRC" rev-parse --verify "\${REMOTE_SOURCE_COMMIT}^{tree}")
if [ "\$REMOTE_SOURCE_TREE" != '${SOURCE_TREE}' ]; then
    echo "remote_agent: source_identity_failure=source_tree_mismatch source tree mismatch: expected ${SOURCE_TREE}, got \$REMOTE_SOURCE_TREE" >&2
    exit 14
fi
SOURCE_COMMIT='${SOURCE_COMMIT}'
SOURCE_TREE='${SOURCE_TREE}'
# Gate-owned root paths are overwritten or excluded below. If the immutable
# source snapshot owns one, runtime input would silently replace source bytes.
git -C "\$SRC" ls-tree -rz --name-only "\$SOURCE_COMMIT" |
while IFS= read -r -d '' _source_path; do
    case "\$_source_path" in
        .brief.md|.schema.json|.agent-result.json|.grok-result.json|.agent-phases.json|.grok-phases.json|.grok-run.log|.grok-debug.log|.grok-selfverify.json|.grok-selfverify.log|.venv|.venv/*|.workbay-lane-sandbox|.workbay-source-provenance|.agent-stream.jsonl)
            echo "remote_agent: source_identity_failure=source_reserved_path_collision path=\$_source_path" >&2
            exit 15
            ;;
    esac
done
git -C "\$SRC" archive "\$SOURCE_COMMIT" | tar -x -C "\$SBX"
_archive_end=\$(date +%s)
_phase_record archive_extract "\$_archive_start" "\$_archive_end"
cd "\$SBX"
_git_init_start=\$(date +%s)
git init -q
# A history-stripped repository otherwise inherits the VM's init.defaultBranch
# (commonly master), so the worker sees the wrong branch despite --branch.
# BRANCH is validated before the local push and expanded while constructing this
# remote body; materialize that exact requested ref before the base commit.
git symbolic-ref HEAD 'refs/heads/${BRANCH}'
git config user.email sandbox@grok.invalid
git config user.name grok-sandbox
# Keep sandbox-runtime files out of git so grok's own 'git add -A' cannot
# sweep the brief/schema/logs/marker into its commit and pollute the returned patch.
printf '%s\n' .brief.md .schema.json .agent-result.json .grok-result.json .agent-phases.json .grok-phases.json .grok-run.log .grok-debug.log .grok-selfverify.json .grok-selfverify.log .venv .workbay-lane-sandbox .workbay-source-provenance .agent-stream.jsonl .review/ > .git/info/exclude
git add -A
# Re-stage the verified source manifest with force so a source-tracked path
# remains tracked even when the archived .gitignore now matches it. Gate-owned
# runtime artifacts stay excluded from the synthetic history [DATA-10][DATA-16].
git -C "\$SRC" ls-tree -rz --name-only "\$SOURCE_COMMIT" | \
    git --literal-pathspecs add -f --pathspec-from-file=- --pathspec-file-nul
git -c commit.gpgsign=false commit -q -m 'sandbox base (${LANE_KEY}, history-stripped, remote-severed)'
git checkout -B '${BRANCH}'
[ "\$(git remote | wc -l)" -eq 0 ] || { echo 'remote_agent: SANDBOX NOT REMOTE-SEVERED — aborting' >&2; exit 1; }
SANDBOX_BASE_COMMIT=\$(git rev-parse --verify HEAD^{commit})
SANDBOX_BASE_TREE=\$(git rev-parse --verify "\${SANDBOX_BASE_COMMIT}^{tree}")
if [ "\$SANDBOX_BASE_TREE" != "\$REMOTE_SOURCE_TREE" ]; then
    echo "remote_agent: source_identity_failure=source_sandbox_tree_mismatch verified source tree \$REMOTE_SOURCE_TREE differs from sandbox base tree \$SANDBOX_BASE_TREE" >&2
    exit 16
fi
# The synthetic base is a root commit, so git would report a complete
# one-commit history. Record that commit as the shallow boundary. Ancestry
# checks then see the stripped history, and the commit object stays present
# for later BASE..HEAD format-patch.
printf '%s\n' "\$SANDBOX_BASE_COMMIT" > .git/shallow
# This untracked, excluded sidecar is created by the gate before the agent
# starts. It maps the verified source object onto the synthetic base while
# remaining outside both source and gate-owned runtime history.
printf 'source_commit=%s\nsource_tree=%s\nsandbox_base_commit=%s\nsandbox_base_tree=%s\n' \
    "\$SOURCE_COMMIT" "\$SOURCE_TREE" "\$SANDBOX_BASE_COMMIT" "\$SANDBOX_BASE_TREE" \
    > .workbay-source-provenance
_git_init_end=\$(date +%s)
_phase_record git_init "\$_git_init_start" "\$_git_init_end"
# Gate-authored durable receipt. This complete chain remains meaningful after
# both the history-stripped sandbox and nonce ref are removed:
# AgentSpec head -> exact requested ref label -> nonce dispatch ref -> verified
# remote commit/tree -> synthetic sandbox commit/tree.
printf '{"schema_version":1,"dispatch_nonce":"%s","requested_branch":"%s","requested_ref":"refs/heads/%s","agent_spec_head_sha":"%s","dispatch_ref":"refs/heads/%s-%s","remote_source_commit":"%s","remote_source_tree":"%s","sandbox_base_commit":"%s","sandbox_base_tree":"%s","history_stripped":true}\n' \
    '${DISPATCH_NONCE}' '${BRANCH}' '${BRANCH}' '${SOURCE_COMMIT}' '${LANE_KEY}' '${DISPATCH_NONCE}' \
    "\$REMOTE_SOURCE_COMMIT" "\$REMOTE_SOURCE_TREE" "\$SANDBOX_BASE_COMMIT" "\$SANDBOX_BASE_TREE" \
    > "\$OUT_DIR/.workbay-source-provenance.json"
BASE="\$SANDBOX_BASE_COMMIT"
cp "\$ROOT/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md" .brief.md
cp "\$ROOT/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json" .schema.json
# </dev/null: uv inherits the bash -s script stream on fd0 like grok does —
# a stdin-reading child would eat the unread script tail (silent truncation).
# Persist the venv across the sandbox wipe by pointing uv at the external
# per-lane env (LANE_VENV, above); exported so grok's own 'uv run' self-verify
# inherits it too. Fail-open: a stale/corrupt reused venv must never wedge the
# lane, so on first-sync failure rebuild it fresh once and retry before
# treating the failure as fatal.
export UV_PROJECT_ENVIRONMENT="\$LANE_VENV"
# BEGIN UV_SYNC_CACHE_FALLBACK
# Prefer the local uv cache so a complete VM cache is deterministic even while
# DNS is unavailable. A cache miss gets one tightly bounded network attempt;
# without the bound tool, refuse an unbounded DNS wait and return the same named
# setup failure. uv itself also receives short HTTP retry/timeout limits.
UV_BIN="\$HOME/.local/bin/uv"
UV_SYNC_NETWORK_TIMEOUT_SEC=45
_uv_sync_with_cache_fallback() {
    local -a _uv_project_args=()
    if [ -n "\${_UV_PROJECT_DIR:-}" ]; then
        _uv_project_args=(--project "\$_UV_PROJECT_DIR")
    fi
    _UV_SYNC_FAILURE_RC=1
    _uv_sync_capture() {
        local _uv_log _uv_rc
        _uv_log=\$(mktemp "\${TMPDIR:-/tmp}/remote-agent-uv-sync.XXXXXX") || {
            _UV_SYNC_FAILURE_RC=1
            echo 'remote_agent: could not capture uv sync stderr' >&2
            return 1
        }
        if "\$@" >"\$_uv_log" 2>&1 </dev/null 9>&-; then
            cat "\$_uv_log" >&2 || true
            rm -f "\$_uv_log" 2>/dev/null || true
            return 0
        else
            _uv_rc=\$?
        fi
        cat "\$_uv_log" >&2 || true
        _UV_SYNC_FAILURE_RC="\$_uv_rc"
        rm -f "\$_uv_log" 2>/dev/null || true
        return "\$_uv_rc"
    }
    if _uv_sync_capture "\$UV_BIN" sync "\${_uv_project_args[@]+"\${_uv_project_args[@]}"}" --offline -q; then
        return 0
    fi
    echo 'remote_agent: offline uv cache incomplete — trying bounded network sync' >&2
    if command -v timeout >/dev/null 2>&1 && \
        UV_HTTP_TIMEOUT=10 UV_HTTP_RETRIES=1 _uv_sync_capture timeout -k 5 "\${UV_SYNC_NETWORK_TIMEOUT_SEC}" \
            "\$UV_BIN" sync "\${_uv_project_args[@]+"\${_uv_project_args[@]}"}" -q; then
        return 0
    fi
    printf 'remote_agent: uv sync failed (uv_sync_failed) uv_rc=%s\n' "\$_UV_SYNC_FAILURE_RC" >&2
    return 1
}
# END UV_SYNC_CACHE_FALLBACK
# Lockfile-hash sync gate (internal S2): skip 'uv sync'
# entirely when uv.lock + every pyproject.toml are byte-identical to the last
# successful sync for this lane AND the persisted venv still exists. The stamp
# lives outside \$SBX (survives the per-pass wipe), keyed by LANE_KEY. Fail-open:
# if sha256sum is unavailable the hash is empty and we always sync (today's
# behavior); any dependency edit changes the hash and forces a re-sync.
_origin_guard_refuse() {
    local reason="\$1"
    echo "remote_agent: venv_origin_shadow \$reason" >&2
    _ORIGIN_GUARD_STATUS=refused
    _PHASES_JSON_PARTS="\${_PHASES_JSON_PARTS:+\${_PHASES_JSON_PARTS},}\"origin_guard\":{\"origin_guard\":\"refused\",\"reason\":\"\$reason\"}"
}
SYNC_STAMP="\$ROOT/.venv-sync-stamp-${LANE_KEY}"
_dep_hash=""
_sync_start=\$(date +%s)
_HAS_LANE_PYPROJECT=0
if [ -f "\$SBX/pyproject.toml" ]; then _HAS_LANE_PYPROJECT=1; fi
# Guarded source trees still require provisioning and origin attestation even
# when their workspace root has no pyproject.toml.
_NEEDS_LANE_PROVISIONING=\$_HAS_LANE_PYPROJECT
_UV_PROJECT_DIR=""
_UV_PROJECT_REFUSAL=""
if [ "\$_HAS_LANE_PYPROJECT" = 1 ]; then _UV_PROJECT_DIR="\$SBX"; fi
for _guarded_source in "\$SBX"/packages/*/src/workbay_*/__init__.py \
    "\$SBX"/packages/*/workbay_*/__init__.py \
    "\$SBX"/packages/*/src/workbay_*.py "\$SBX"/packages/*/workbay_*.py; do
    [ -f "\$_guarded_source" ] || continue
    _NEEDS_LANE_PROVISIONING=1
    [ "\$_HAS_LANE_PYPROJECT" = 0 ] || continue
    # Walk upwards only inside the sandbox, choosing the nearest manifest.
    _project_candidate="\${_guarded_source%/*}"
    while [ "\$_project_candidate" != "\$SBX" ] && [ ! -f "\$_project_candidate/pyproject.toml" ]; do
        _project_candidate="\${_project_candidate%/*}"
    done
    if [ ! -f "\$_project_candidate/pyproject.toml" ]; then
        _UV_PROJECT_REFUSAL=no_project_manifest
    elif [ -z "\$_UV_PROJECT_DIR" ]; then
        _UV_PROJECT_DIR="\$_project_candidate"
    elif [ "\$_UV_PROJECT_DIR" != "\$_project_candidate" ] && [ "\$_UV_PROJECT_REFUSAL" != no_project_manifest ]; then
        _UV_PROJECT_REFUSAL=multiple_project_manifests
    fi
done
if [ "\$_NEEDS_LANE_PROVISIONING" = 1 ]; then
if [ -n "\$_UV_PROJECT_REFUSAL" ]; then
    _phase_record uv_sync "\$_sync_start" "\$(date +%s)" refused_no_manifest
    _origin_guard_refuse "\$_UV_PROJECT_REFUSAL"
    echo 'remote_agent: uv sync failed (uv_sync_failed) reason=no_project_manifest' >&2
    exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
    _dep_hash=\$( { cat uv.lock 2>/dev/null; find . -name pyproject.toml -not -path './.venv/*' 2>/dev/null | sort | xargs cat 2>/dev/null; } | sha256sum | cut -d' ' -f1 )
fi
if [ -n "\$_dep_hash" ] && [ -d "\$LANE_VENV" ] && [ "\$(cat "\$SYNC_STAMP" 2>/dev/null)" = "\$_dep_hash" ]; then
    echo 'remote_agent: uv.lock+pyproject unchanged for lane — skipping uv sync (warm venv)' >&2
    _PHASES_WARM_SKIP=1
else
    if ! _uv_sync_with_cache_fallback; then
        echo 'remote_agent: uv sync failed against reused venv — rebuilding fresh and retrying' >&2
        rm -rf "\$LANE_VENV"
        _uv_sync_with_cache_fallback || exit 1
        echo 'remote_agent: uv sync recovered (uv_sync_recovered)' >&2
    fi
    # Stamp the dep hash only AFTER a successful sync so an aborted/failed sync
    # never records a warm-skip for a half-populated venv (fail-open to re-sync).
    [ -n "\$_dep_hash" ] && printf '%s\n' "\$_dep_hash" > "\$SYNC_STAMP" 2>/dev/null || true
fi
# VM-LANE-VENV-EDITABLE-SYNC-SHIPS-SCRUBBED-COPY-SHADOWS-WORKTREE-SOURCES-01:
# Probe the actual install in isolation: inherited PYTHONPATH must not hide a
# frozen site-packages copy. Emit evidence BEFORE removing the derived venv.
_ORIGIN_GUARD_STATUS=''
_assert_lane_venv_origin() {
    _ORIGIN_GUARD_STATUS=''
    local _origin_probe_status _origin_nonce
    _origin_nonce=\$(od -An -N16 -tx1 /dev/urandom | tr -d " \n") || { _origin_guard_refuse nonce_unavailable; return 1; }
    [ -n "\$_origin_nonce" ] || { _origin_guard_refuse nonce_unavailable; return 1; }
    _origin_probe_status=\$(timeout -k 5 30 "\$LANE_VENV/bin/python" -I -c '
import importlib
import importlib.util
import importlib.machinery
import os
import pkgutil
import re
import sys
from pathlib import Path

packages_path = Path(sys.argv[1])
expected = packages_path.resolve()
worktree = packages_path.parent.resolve()
failed = False
# Discover both supported layouts; metadata cannot attest the executing file.
names = sorted({
    path.parent.name
    for pattern in ("*/src/workbay_*/__init__.py", "*/workbay_*/__init__.py")
    for path in expected.glob(pattern)
} | {
    path.stem
    for pattern in ("*/src/workbay_*.py", "*/workbay_*.py")
    for path in expected.glob(pattern)
})
# Inventory the whole tree, distinguishing repository helpers from installed roots.
def unknown_layout(path):
    print("unknown_layout")
    print("remote_agent: venv_origin_shadow unknown_layout", path, file=sys.stderr)
    sys.exit(1)

helpers = set()
ignored = {".venv", "node_modules", ".git", "__pycache__"}
def skip_entry(path):
    # Nested venvs (.venv, .venv-handoff, .venv-lane-*) are not source layouts.
    # Linux platlib is often lib64 (symlink to lib, or a real lib64 dir).
    if path.name in ignored or path.name.startswith(".venv"):
        return True
    if (path / "pyvenv.cfg").is_file():
        return True
    if path.name in ("lib", "lib64"):
        parent = path.parent
        if (parent / "pyvenv.cfg").is_file() or parent.name.startswith(".venv"):
            return True
    return False
def platlib_alias(path):
    if path.name not in ("lib", "lib64") or not path.is_symlink():
        return False
    target = Path(os.path.realpath(path))
    return target.name in ("lib", "lib64") and target.parent == path.parent.resolve()
hidden_names = set()
def known_system_alias(path):
    # macOS firmlink aliases only. Compare unresolved path to resolved target.
    if not path.is_symlink():
        return False
    resolved = Path(os.path.realpath(path))
    allowed = {
        Path("/tmp"): Path("/private/tmp"),
        Path("/var"): Path("/private/var"),
    }
    return allowed.get(Path(os.path.normpath(str(path)))) == resolved
def worktree_redirected(path):
    if path.is_symlink() or path.parent.is_symlink():
        return True
    for ancestor in path.parent.parents:
        if ancestor.is_symlink() and not known_system_alias(ancestor):
            return True
    return False
def is_importable_workbay_name(name):
    # Top-level identifiers only. dist-info/egg-info/egg-link names are not.
    return name.startswith("workbay_") and name.isidentifier()
def inside_worktree(path):
    if path is None:
        return False
    real = os.path.realpath(str(path))
    root = str(worktree)
    return real == root or real.startswith(root + os.sep)
def candidate_controlled_origin(spec):
    # Origins inside the worktree are candidate-controlled even when the
    # packages walk never visited them (root venv, node_modules venv).
    # Origins outside the worktree are not candidate-controlled.
    if spec is None:
        return False
    spec_origin = getattr(spec, "origin", None)
    if spec_origin not in (None, "namespace") and inside_worktree(spec_origin):
        return True
    for loc in spec.submodule_search_locations or ():
        if inside_worktree(loc):
            return True
    return False
def discover_path_visible_workbay_names():
    # Importable workbay_* names visible to this interpreter, independent
    # of the packages filesystem walk.
    for entry in sys.path:
        if not entry:
            continue
        try:
            for info in pkgutil.iter_modules([entry]):
                if is_importable_workbay_name(info.name):
                    hidden_names.add(info.name)
        except BaseException:
            if inside_worktree(entry):
                unknown_layout(entry)
        try:
            listed = Path(entry)
            if listed.is_dir():
                for child in listed.iterdir():
                    add_hidden_entry(child)
        except BaseException:
            if inside_worktree(entry):
                unknown_layout(entry)
    for finder in sys.meta_path:
        for attr in ("MAPPING", "mapping", "_mapping"):
            mapped = getattr(finder, attr, None)
            if isinstance(mapped, dict):
                for key in mapped:
                    if is_importable_workbay_name(str(key)):
                        hidden_names.add(str(key))
def add_hidden_entry(entry):
    # Directory packages or .py/extension-module files. Skip metadata.
    # .pth and __editable__ files contribute via contents, not their names.
    name = entry.name
    if name.startswith("__editable__"):
        return
    if (name.endswith(".dist-info") or name.endswith(".egg-info")
            or name.endswith(".egg-link") or name.endswith(".pth")):
        return
    if entry.is_dir():
        if is_importable_workbay_name(name):
            hidden_names.add(name)
        return
    for suffix in importlib.machinery.all_suffixes():
        if name.endswith(suffix):
            stem = name[:-len(suffix)]
            if is_importable_workbay_name(stem):
                hidden_names.add(stem)
            return
def collect_hidden_workbay_names(site):
    for entry in site.iterdir():
        add_hidden_entry(entry)
        if entry.suffix == ".pth" or entry.name.startswith("__editable__"):
            try:
                text = entry.read_text()
            except BaseException:
                unknown_layout(entry)
            for match in re.findall("workbay_[A-Za-z0-9_]+", text):
                if is_importable_workbay_name(match):
                    hidden_names.add(match)
            for raw in text.splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or line.startswith("import "):
                    continue
                target = Path(line)
                if not target.is_absolute():
                    target = site / line
                if not target.is_dir():
                    continue
                children = list(target.iterdir())
                src = target / "src"
                if src.is_dir():
                    children.extend(src.iterdir())
                for child in children:
                    add_hidden_entry(child)
def audit_hidden_venv(path):
    # Skipped venv trees are not trusted absence: scan site-packages for
    # workbay_* packages and editable finders, and refuse a lib/lib64
    # symlink that resolves outside this venv.
    if not (path.name.startswith(".venv") or (path / "pyvenv.cfg").is_file()):
        return
    if path.is_symlink() or not path.is_dir():
        unknown_layout(path)
    for libname in ("lib", "lib64"):
        libpath = path / libname
        if libpath.is_symlink():
            target = Path(os.path.realpath(libpath))
            if not (target.name in ("lib", "lib64") and target.parent == path.resolve()):
                unknown_layout(libpath)
            libpath = target
        if not libpath.is_dir():
            continue
        for py_dir in libpath.iterdir():
            if not py_dir.name.startswith("python") or not py_dir.is_dir():
                continue
            site = py_dir / "site-packages"
            if site.is_dir():
                collect_hidden_workbay_names(site)
def inventory(root, depth=0):
    for path in root.iterdir():
        if skip_entry(path):
            audit_hidden_venv(path)
            continue
        if path.is_symlink() or (path / "__init__.py").is_symlink():
            if platlib_alias(path):
                continue
            unknown_layout(path)
        parts = path.relative_to(expected).parts
        if path.name.startswith("workbay_") and (path.suffix == ".py" or
                (path.is_dir() and not (depth == 0 and (path / "src").is_dir()))):
            supported = len(parts) == 2 or (len(parts) == 3 and parts[1] == "src")
            # workbay-system ships CLI helpers under scripts, not editable roots.
            helper = parts == ("workbay-system", "scripts", "workbay_evals")
            if (not supported and not helper) or (path.is_dir() and not (path / "__init__.py").is_file()):
                unknown_layout(path)
            if helper:
                helpers.add(path.stem if path.suffix == ".py" else path.name)
            continue
        if path.is_dir():
            if depth >= 8 or path.is_symlink():
                unknown_layout(path)
            inventory(path, depth + 1)
try:
    # Refuse a redirected packages root or worktree parent. Higher ancestors
    # may be known OS aliases (macOS /tmp -> /private/tmp, /var -> /private/var).
    if (expected.exists() and not expected.is_dir()) or worktree_redirected(packages_path):
        unknown_layout(expected)
    if expected.is_dir():
        inventory(expected)
except BaseException as exc:
    print("unknown_layout")
    print("remote_agent: venv_origin_shadow unknown_layout", type(exc).__name__, file=sys.stderr)
    sys.exit(1)
discover_path_visible_workbay_names()
# Helpers need no installation, but an installed shadow must still be rejected.
for name in sorted(helpers):
    if importlib.util.find_spec(name) is not None:
        names.append(name)
inventoried_names = set(names)
for name in sorted(hidden_names):
    if name in names:
        continue
    try:
        spec = importlib.util.find_spec(name)
    except (ValueError, ImportError):
        # Malformed names must not crash the probe. A real hidden shadow
        # returns a spec and is PathFinder-attested below; an exception
        # means origin cannot be verified, so fail closed.
        unknown_layout(name)
    if spec is not None and candidate_controlled_origin(spec):
        names.append(name)
if not names:
    # internal: a consumer repo ships no workbay_* sources,
    # so there is nothing a scrubbed copy could shadow; the guard is not
    # applicable there. Package METADATA without sources is the scrubbed-copy
    # shadow itself and still fails closed. The glob is "*workbay*", not
    # "workbay*": three package directories here are named mcp-workbay-*
    # (canvas, handoff, orchestrator), so a leading-anchored pattern would
    # classify a scrubbed orchestrator copy as not_applicable and exit 0,
    # failing open in exactly the case this guard exists to catch.
    # NOTE: no apostrophes in this block. It lives inside a single-quoted
    # python -c body, so one apostrophe silently truncates the probe while
    # bash -n on the whole script still passes.
    scrubbed = sorted(
        p.parent.name
        for p in expected.glob("*workbay*/pyproject.toml")
    )
    if scrubbed:
        print("scrubbed_sources")
        print("remote_agent: venv_origin_shadow scrubbed_sources", *scrubbed, file=sys.stderr)
        sys.exit(1)
# Resolve source specs independently of sys.meta_path, from inventoried roots.
source_dirs = sorted({str(p.parent.parent) for pattern in
    ("*/src/workbay_*/__init__.py", "*/workbay_*/__init__.py")
    for p in expected.glob(pattern)} | {str(p.parent) for pattern in
    ("*/src/workbay_*.py", "*/workbay_*.py") for p in expected.glob(pattern)})
source_dirs.append(str(expected / "workbay-system/scripts"))
file_loaders = {importlib.machinery.SourceFileLoader,
                importlib.machinery.ExtensionFileLoader,
                importlib.machinery.SourcelessFileLoader}
def inside(path):
    return path is not None and os.path.realpath(path).startswith(str(expected) + os.sep)

def valid_spec(spec, reference, name):
    if spec is None:
        return False
    if spec.origin in (None, "namespace"):
        unknown_layout(name)
    if type(spec.loader) not in file_loaders:
        print("custom_loader")
        print("remote_agent: venv_origin_shadow custom_loader", name, file=sys.stderr)
        return False
    filename = spec.loader.get_filename(name)
    return (reference is not None and inside(spec.origin) and inside(filename)
            and os.path.realpath(spec.origin) == os.path.realpath(reference.origin)
            and os.path.realpath(filename) == os.path.realpath(reference.origin)
            and tuple(map(os.path.realpath, spec.submodule_search_locations or ()))
                == tuple(map(os.path.realpath, reference.submodule_search_locations or ())))

# Keep dependency-installed instrumentation from changing later root probes.
# Startup hooks are retained and must pass the same loader checks.
startup_path_hooks = sys.path_hooks[:]
startup_meta_path = sys.meta_path[:]
for name in names:
    try:
        reference = importlib.machinery.PathFinder.find_spec(name, source_dirs)
        spec = importlib.util.find_spec(name)
        origin = spec.origin if spec else None
        if name not in inventoried_names and not valid_spec(spec, reference, name):
            unknown_layout(name)
        if valid_spec(spec, reference, name):
            try:
                module = importlib.import_module(name)
            finally:
                sys.path_hooks[:] = startup_path_hooks
                sys.meta_path[:] = startup_meta_path
                sys.path_importer_cache.clear()
            spec = importlib.util.find_spec(name)
            origin = getattr(module, "__file__", None)
            if (valid_spec(spec, reference, name) and inside(origin)
                    and os.path.realpath(origin) == os.path.realpath(reference.origin)
                    and valid_spec(module.__spec__, reference, name)):
                continue
    except BaseException as exc:
        origin = "import_error:" + type(exc).__name__
    print("remote_agent: venv_origin_shadow", name, origin, file=sys.stderr)
    failed = True
# Reuse the lane guard when available, including its editable-shadow diagnostics.
# The file check above is the fail-closed fallback for package-only fixtures.
guard_path = expected / "workbay-system/scripts/pytest_path_guard.py"
if guard_path.is_file():
    import runpy
    try:
        guard = runpy.run_path(str(guard_path))
        for violation in guard["collect_violations"](expected):
            print("remote_agent: venv_origin_shadow", *violation, file=sys.stderr)
            failed = True
    except BaseException as exc:
        print("path_guard_error")
        print("remote_agent: venv_origin_shadow path_guard_error", type(exc).__name__, file=sys.stderr)
        failed = True
if failed:
    sys.exit(1)
if not names:
    print("remote_agent: venv_origin_guard skipped consumer layout has no workbay_* packages", file=sys.stderr)
    print("remote_agent: venv_origin_guard not_applicable", file=sys.stderr)
print(sys.argv[2] + ":" + ("verified_roots" if names else "skipped_no_guarded_roots"))
' "\$SBX/packages" "\$_origin_nonce" </dev/null 9>&-) || {
        case "\$_origin_probe_status" in
            *custom_loader*) _origin_guard_refuse custom_loader ;;
            *unknown_layout*) _origin_guard_refuse unknown_layout ;;
            *scrubbed_sources*) _origin_guard_refuse scrubbed_sources ;;
            *path_guard_error*) _origin_guard_refuse path_guard_error ;;
            *) _origin_guard_refuse probe_failed_or_timed_out ;;
        esac
        return 1
    }
    case "\$_origin_probe_status" in
        "\$_origin_nonce:verified_roots") _ORIGIN_GUARD_STATUS=verified_roots ;;
        "\$_origin_nonce:skipped_no_guarded_roots") _ORIGIN_GUARD_STATUS=skipped_no_guarded_roots ;;
        *) _origin_guard_refuse invalid_attestation; return 1 ;;
    esac
}
if ! _assert_lane_venv_origin; then
    echo 'remote_agent: lane import origin invalid — rebuilding fresh once' >&2
    rm -rf "\$LANE_VENV" "\$SYNC_STAMP"
    _PHASES_WARM_SKIP=0
    # Origin repair has one sync attempt; keep its failure marker last on stderr.
    _origin_uv_project_args=()
    if [ -n "\${_UV_PROJECT_DIR:-}" ]; then
        _origin_uv_project_args=(--project "\$_UV_PROJECT_DIR")
    fi
    if "\$UV_BIN" sync "\${_origin_uv_project_args[@]+"\${_origin_uv_project_args[@]}"}" -q </dev/null 9>&-; then
        :
    else
        _UV_SYNC_FAILURE_RC=\$?
        printf 'remote_agent: uv sync failed (uv_sync_failed) uv_rc=%s\n' "\$_UV_SYNC_FAILURE_RC" >&2
        exit 1
    fi
    _assert_lane_venv_origin || { echo 'remote_agent: uv sync failed to repair lane import origin' >&2; exit 1; }
    [ -n "\$_dep_hash" ] && printf '%s\n' "\$_dep_hash" > "\$SYNC_STAMP" 2>/dev/null || true
fi
else
    _ORIGIN_GUARD_STATUS=skipped_no_pyproject
    echo 'remote_agent: uv sync skipped (no root pyproject.toml)' >&2
fi
# Pin only this sandbox's source roots for both the agent and self-verify.
# Use a shell array to preserve spaces and avoid an empty cwd path entry.
_lane_src_dirs=()
for _lane_src in "\$SBX"/packages/*/src; do
    [ ! -d "\$_lane_src" ] || _lane_src_dirs+=("\$_lane_src")
done
# Flat projects contribute their project root, after all src layouts.
for _lane_init in "\$SBX"/packages/*/workbay_*/__init__.py "\$SBX"/packages/*/workbay_*.py; do
    [ -f "\$_lane_init" ] || continue
    case "\$_lane_init" in
        */__init__.py) _lane_flat="\${_lane_init%/*/*}" ;;
        *) _lane_flat="\${_lane_init%/*}" ;;
    esac
    case ":\$(IFS=:; printf '%s' "\${_lane_src_dirs[*]}"):" in
        *":\$_lane_flat:"*) ;;
        *) _lane_src_dirs+=("\$_lane_flat") ;;
    esac
done
export PYTHONPATH=\$(IFS=:; printf '%s' "\${_lane_src_dirs[*]}")
_sync_end=\$(date +%s)
if [ "\$_NEEDS_LANE_PROVISIONING" = 1 ]; then
    _phase_record sync "\$_sync_start" "\$_sync_end"
else
    _phase_record uv_sync "\$_sync_start" "\$_sync_end" skipped_no_pyproject
fi
# Add the attestation to the sync phase just recorded (remove its closing }).
case "\$_ORIGIN_GUARD_STATUS" in
    verified_roots) _origin_ok=1 ;;
    skipped_no_guarded_roots) _origin_ok=null ;;
    skipped_no_pyproject) _origin_ok=null ;;
    *) _origin_guard_refuse invalid_attestation; exit 1 ;;
esac
_PHASES_JSON_PARTS="\${_PHASES_JSON_PARTS%?},\"origin_ok\":\$_origin_ok,\"origin_guard\":\"\$_ORIGIN_GUARD_STATUS\"}"
# agent_launch opens at end of last pre-agent phase that ran (sync on S1 path).
_AGENT_LAUNCH_OPEN_TS=\$_sync_end
# Back-compat: expose the persistent env at the conventional \$SBX/.venv path
# (symlink) so any '.venv/bin'-relative self-verify still resolves. Excluded
# from git above so it cannot pollute grok's patch. Best-effort, non-fatal.
ln -sfn "\$LANE_VENV" "\$SBX/.venv" 2>/dev/null || true
# OFFLOAD-LANEVENV-DANGLING-SYMLINK-01: LRU reap of another lane can delete
# \$LANE_VENV under a live sandbox, leaving \$SBX/.venv dangling and PATH
# silently resolving nothing (bare python → exit 127 harness_error). Detect,
# rebuild once, and refuse to claim a PATH contract we cannot honour.
_ensure_lane_venv_symlink() {
    if [ "\$_NEEDS_LANE_PROVISIONING" = 0 ]; then
        return 0
    fi
    if [ -d "\$LANE_VENV/bin" ]; then
        if [ ! -d "\$SBX/.venv/bin" ]; then
            ln -sfn "\$LANE_VENV" "\$SBX/.venv" 2>/dev/null || true
        fi
        if [ -d "\$SBX/.venv/bin" ]; then
            return 0
        fi
    fi
    if [ -L "\$SBX/.venv" ] && [ ! -e "\$SBX/.venv" ]; then
        echo 'remote_agent: \$SBX/.venv is a dangling symlink (lane venv missing)' >&2
    else
        echo 'remote_agent: lane venv missing or incomplete — rebuilding' >&2
    fi
    rm -f "\$SBX/.venv" 2>/dev/null || true
    rm -rf "\$LANE_VENV" 2>/dev/null || true
    _uv_sync_with_cache_fallback || return 1
    ln -sfn "\$LANE_VENV" "\$SBX/.venv" 2>/dev/null || true
    if [ ! -d "\$SBX/.venv/bin" ]; then
        echo 'remote_agent: \$SBX/.venv still unusable after rebuild — PATH contract would lie' >&2
        echo 'remote_agent: uv sync failed — lane venv unusable' >&2
        return 1
    fi
    return 0
}
# uv can exit successfully without leaving a usable venv. Keep that setup
# failure typed without inventing a nonzero uv exit code.
_ensure_lane_venv_symlink || exit 1
# A broken reused venv can contain both editable metadata and a stale plain
# package directory. That directory wins normal import resolution and makes
# self-verification exercise old code. Remove only positively identified
# shadows, reinstall that distribution once, and fail retryably if it returns.
# --- workbay: venv-shadow-guard-functions (begin) ---
_editable_venv_shadow_packages() {
    _shadow_mode="\$1"
    timeout -k 5 30 "\$LANE_VENV/bin/python" -I -c '
import json
import re
import shutil
import sys
from pathlib import Path

venv = Path(sys.argv[1])
mode = sys.argv[2]
if mode not in ("scan", "remove"):
    raise SystemExit("invalid shadow scan mode")
site_dirs = sorted(venv.glob("lib/python*/site-packages"))
site_dirs += sorted(venv.glob("lib64/python*/site-packages"))
found = set()
for site_dir in site_dirs:
    for info in sorted(site_dir.glob("*.dist-info")):
        direct_path = info / "direct_url.json"
        if not direct_path.is_file():
            continue
        direct = json.loads(direct_path.read_text(encoding="utf-8"))
        editable = direct.get("editable") is True
        dir_info = direct.get("dir_info")
        if isinstance(dir_info, dict):
            editable = editable or dir_info.get("editable") is True
        if not editable:
            continue
        distribution = ""
        metadata_path = info / "METADATA"
        if metadata_path.is_file():
            for line in metadata_path.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.lower().startswith("name:"):
                    distribution = line.split(":", 1)[1].strip()
                    break
        if not distribution:
            distribution = info.name.rsplit(".dist-info", 1)[0].rsplit("-", 1)[0]
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", distribution):
            raise RuntimeError("unsafe editable distribution name: " + distribution)
        top_level = info / "top_level.txt"
        if top_level.is_file():
            imports = top_level.read_text(encoding="utf-8").splitlines()
        else:
            # Distribution names do not establish import ownership. Missing
            # metadata is no evidence permitting removal of a plain directory.
            imports = []
        for import_name in imports:
            import_name = import_name.strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", import_name):
                continue
            candidate = site_dir / import_name
            if candidate.is_dir() and not candidate.is_symlink():
                found.add(distribution)
                if mode == "scan":
                    print("remote_agent: editable shadow metadata:", info, file=sys.stderr)
                    print("remote_agent: site-packages listing:",
                          ", ".join(sorted(p.name for p in site_dir.iterdir())),
                          file=sys.stderr)
                if mode == "remove":
                    shutil.rmtree(candidate)
for distribution in sorted(found):
    print(distribution)
' "\$LANE_VENV" "\$_shadow_mode" </dev/null 9>&-
}

_guard_editable_venv_shadows() {
    if [ "\$_NEEDS_LANE_PROVISIONING" = 0 ]; then
        return 0
    fi
    _shadow_removed="\$(_editable_venv_shadow_packages remove)" || {
        echo 'remote_agent: editable venv shadow inspection/removal failed' >&2
        return 75
    }
    for _shadow_package in \$_shadow_removed; do
        if ! "\$HOME/.local/bin/uv" sync -q --reinstall-package "\$_shadow_package" >&2 </dev/null 9>&-; then
            echo "remote_agent: editable venv shadow reinstall failed for \$_shadow_package" >&2
            return 75
        fi
    done
    _shadow_persisting="\$(_editable_venv_shadow_packages scan)" || {
        echo 'remote_agent: editable venv shadow recheck failed' >&2
        return 75
    }
    if [ -n "\$_shadow_persisting" ]; then
        _shadow_names="\$(printf '%s\n' "\$_shadow_persisting" | tr '\n' ',' | sed 's/,$//')"
        echo "remote_agent: editable_venv_shadow_persists after reinstall: \$_shadow_names" >&2
        return 75
    fi
    if [ -n "\$_shadow_removed" ]; then
        _shadow_names="\$(printf '%s\n' "\$_shadow_removed" | tr '\n' ',' | sed 's/,$//')"
        echo "remote_agent: venv_shadow_removed=\$_shadow_names" >&2
    fi
    return 0
}
# --- workbay: venv-shadow-guard-functions (end) ---
_guard_editable_venv_shadows || { _shadow_rc=\$?; exit "\$_shadow_rc"; }

# Bound the networked host-side cache only after every possible sync/reinstall.
# uv cache prune is reversible through re-download and retains used entries;
# cache clean is intentionally never automatic. Timeout is the sweep bulkhead.
UV_CACHE_CAP_MB="\${WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB:-4096}"
# --- workbay: uv-cache-bound-function (begin) ---
_bound_uv_cache() {
    [ "\$UV_CACHE_CAP_MB" -gt 0 ] || return 0
    # Keep every no-follow component open until the receipt is committed.
    # Python's errno-aware openat distinguishes ENOENT from lookup failures;
    # shell predicates cannot. No shell cd/CDPATH/OLDPWD is involved.
    if python3 - "\${UV_CACHE_DIR:-\$HOME/.cache/uv}" "\$UV_CACHE_CAP_MB" "\$HOME/.local/bin/uv" "\${_BOUND_DEADLINE:-}" <<'PYUVCACHE'
import ctypes
import errno
import os
import shutil
import signal
import stat
import subprocess
import sys
import time

cache, cap_mb, uv, body_deadline = sys.argv[1:]
fds = []
bindings = []
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
helper = None


def remaining():
    left = work_deadline - time.monotonic()
    if left <= 0:
        raise OSError("uv cache deadline exhausted")
    return left


def expired(signum, frame):
    raise OSError("uv cache deadline exhausted")


def signal_owned(sig):
    # This single-threaded, dedicated subreaper launches only this helper.
    # Its direct children are therefore owned, including adopted descendants
    # that changed session/group. No wait/reap occurs between discovery and
    # pidfd_open: an exited child retains its PID until we reap it ourselves.
    with open(f"/proc/self/task/{os.getpid()}/children") as children:
        pids = children.read().split()
    for pid in pids:
        if time.monotonic() >= finish_deadline:
            raise OSError("uv cache owned child cleanup deadline exhausted")
        pidfd = libc.pidfd_open(int(pid), 0)
        if pidfd < 0:
            raise OSError(ctypes.get_errno(), "cache owned child pidfd unavailable")
        try:
            if libc.pidfd_send_signal(pidfd, sig, None, 0) != 0:
                error = ctypes.get_errno()
                if error != errno.ESRCH:
                    raise OSError(error, "cache owned child signal failed")
        finally:
            os.close(pidfd)


def cleanup_owned():
    global helper
    # [RES-03] Slow failure worse than fast failure (Release It!, ch. 5):
    # TERM, then finite kill-after, inside the original lifecycle budget.
    # Killing direct children exposes their descendants to this subreaper.
    # Repeat discovery/signalling/reaping until ECHILD for ALL children;
    # ECHILD for the original process group cannot prove ownership is empty.
    signal.setitimer(signal.ITIMER_REAL, 0)
    grace_end = min(time.monotonic() + cleanup_reserve / 4, finish_deadline)
    while True:
        signal_owned(signal.SIGTERM if time.monotonic() < grace_end else signal.SIGKILL)
        while True:
            if time.monotonic() >= finish_deadline:
                raise OSError("uv cache owned child cleanup deadline exhausted")
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                helper = None
                return
            if helper is not None and pid == helper.pid:
                helper.returncode = os.waitstatus_to_exitcode(status)
            if pid == 0:
                break
        time.sleep(min(0.01, max(0, finish_deadline - time.monotonic())))


def run_owned(argv, **kwargs):
    global helper
    remaining()
    try:
        helper = subprocess.Popen(argv, start_new_session=True, **kwargs)
        stdout, stderr = helper.communicate(timeout=remaining())
        return subprocess.CompletedProcess(argv, helper.returncode, stdout, stderr)
    except subprocess.TimeoutExpired as error:
        raise OSError("uv cache deadline exhausted") from error
    finally:
        cleanup_owned()
        signal.setitimer(signal.ITIMER_REAL, remaining())


def identity(info):
    return info.st_dev, info.st_ino


def validate():
    remaining()
    for parent, name, child in bindings:
        remaining()
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if not stat.S_ISDIR(current.st_mode) or identity(current) != identity(os.fstat(child)):
            raise OSError("cache path identity changed")
    for fd in fds:
        remaining()
        if not os.access(".", os.R_OK | os.X_OK, dir_fd=fd):
            raise OSError("cache directory is inaccessible")


def inspect():
    global effective_cache
    path = cache if cache.startswith("/") else os.getcwd() + "/" + cache
    # UV expands relative operands from the physical cwd and normalizes the
    # resulting absolute path. Keep raw traversal/checks as well: normalization
    # must not hide an invalid visited component or a symlink.
    effective_cache = os.path.normpath(path)
    parent = os.open("/", flags)
    fds.append(parent)
    parts = path.split("/")
    for index, name in enumerate(parts):
        remaining()
        if not name:
            continue
        validate()
        try:
            child = os.open(name, flags, dir_fd=parent)
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
            # Only absence under the still-bound, accessible parent is empty.
            validate()
            try:
                os.stat(name, dir_fd=parent, follow_symlinks=False)
            except OSError as missing:
                if missing.errno == errno.ENOENT:
                    validate()
                    # Native ENOENT already proves this name is lookup-valid;
                    # validate every component that lookup has not reached.
                    validate_lengths(parent, parts[index + 1:])
                    # A missing directory may be created by UV before ".."
                    # returns to a populated or unsafe existing namespace.
                    # Length checks alone cannot prove that destination empty;
                    # refuse unresolved navigation before launching any helper.
                    if ".." in parts[index + 1:]:
                        raise OSError("cache missing suffix has unresolved parent navigation")
                    return None
                raise
            raise OSError("cache path appeared during inspection")
        fds.append(child)
        bindings.append((parent, name, child))
        parent = child
    validate()
    validate_lengths(parent, [])
    return parent


def validate_lengths(parent, suffix):
    # ENOENT proves only one missing component, not a creatable suffix.
    # Query the held reachable filesystem; pathconf counts bytes including
    # the pathname's NUL terminator. Unknown limits are not empty evidence.
    remaining()
    path_max = os.fpathconf(parent, "PC_PATH_MAX")
    name_max = os.fpathconf(parent, "PC_NAME_MAX")
    if path_max <= 0 or name_max <= 0:
        raise OSError("cache filesystem pathname limits unavailable")
    if max(len(os.fsencode(cache)), len(os.fsencode(effective_cache))) >= path_max:
        raise OSError(errno.ENAMETOOLONG, "cache pathname exceeds filesystem byte limit")
    for name in suffix:
        remaining()
        if len(os.fsencode(name)) > name_max:
            raise OSError(errno.ENAMETOOLONG, "cache suffix exceeds filesystem name byte limit")


def measure(fd, phase):
    validate()
    # The trailing slash asks du to measure the held directory, not procfs's
    # descriptor symlink. Only this descriptor is inherited by each child.
    result = run_owned(["du", "-sk", "--", f"/proc/self/fd/{fd}/."],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            pass_fds=(fd,))
    validate()
    rows = result.stdout.splitlines()
    fields = rows[0].split() if len(rows) == 1 else []
    if result.returncode or not fields or not fields[0].isdigit():
        raise OSError(f"{phase} du failed or returned a non-numeric size")
    return int(fields[0])


try:
    # One absolute monotonic deadline for inspection, both measurements,
    # pruning, and owned-child cleanup. The existing body clock can shorten,
    # never extend, the cache phase's maximum 90 seconds. Standalone callers
    # without that clock retain the same finite maximum.
    budget = min(90.0, float(body_deadline) - time.time()) if body_deadline else 90.0
    if not 0 < budget <= 90:
        raise OSError("uv cache deadline exhausted or invalid")
    finish_deadline = time.monotonic() + budget
    cleanup_reserve = min(1.0, budget / 4)
    work_deadline = finish_deadline - cleanup_reserve
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, remaining())
    # Linux is already required by the descriptor-bound /proc operand.
    # Adopt only descendants of this short-lived guard, never shared helpers.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "cache helper subreaper unavailable")
    if not hasattr(libc, "pidfd_open") or not hasattr(libc, "pidfd_send_signal"):
        raise OSError("cache owned child pidfd support unavailable")
    libc.pidfd_open.argtypes = [ctypes.c_int, ctypes.c_uint]
    libc.pidfd_open.restype = ctypes.c_int
    libc.pidfd_send_signal.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    libc.pidfd_send_signal.restype = ctypes.c_int
    # Refuse unsupported kernels before creating any owned external process.
    probe = libc.pidfd_open(os.getpid(), 0)
    if probe < 0:
        raise OSError(ctypes.get_errno(), "cache owned child pidfd unavailable")
    os.close(probe)
    fd = inspect()
    if fd is None:
        before_mb = after_mb = 0
    else:
        before_kb = measure(fd, "pre-prune")
        before_mb = after_mb = (before_kb + 1023) // 1024
        if before_kb > int(cap_mb) * 1024:
            if shutil.which("timeout") is None:
                raise OSError("timeout unavailable for uv cache prune")
            validate()
            # [RES-02] Timeout on every blocking call (Release It!, ch. 5):
            # supervise timeout 90 with the one remaining absolute deadline.
            # An explicit uv option overrides the ambient cache path, so a
            # rename at the exec boundary cannot redirect destructive pruning.
            result = run_owned(["timeout", "90", uv, "cache", "prune",
                                     "--cache-dir", f"/proc/self/fd/{fd}"],
                                    stdin=subprocess.DEVNULL, stdout=sys.stderr,
                                    stderr=sys.stderr, pass_fds=(fd,))
            validate()
            if result.returncode:
                raise OSError("bounded uv cache prune failed")
            after_mb = (measure(fd, "post-prune") + 1023) // 1024
    validate()
    print(f"remote_agent: uv_cache_mb_before={before_mb} uv_cache_mb_after={after_mb}",
          file=sys.stderr)
except (OSError, ValueError) as error:
    print(f"remote_agent: uv_cache_unmeasurable: {error}", file=sys.stderr)
    sys.exit(75)
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
    for fd in reversed(fds):
        os.close(fd)
PYUVCACHE
    then
        return 0
    else
        echo 'remote_agent: uv_cache_unmeasurable: cache binding failed' >&2
        return 75
    fi
}
# --- workbay: uv-cache-bound-function (end) ---
_uv_cache_path="\${UV_CACHE_DIR:-\$HOME/.cache/uv}"
if [ "\$_NEEDS_LANE_PROVISIONING" = 0 ] && [ ! -e "\$_uv_cache_path" ] && [ ! -L "\$_uv_cache_path" ]; then
    _uv_cache_skip_ts=\$(date +%s)
    _phase_record uv_cache "\$_uv_cache_skip_ts" "\$_uv_cache_skip_ts" skipped_no_cache
else
    _bound_uv_cache || { _uv_cache_rc=\$?; exit "\$_uv_cache_rc"; }
fi
# An OOM-killed prior run leaves ${LANE_UNIT} in systemd 'failed' state,
# which refuses the unit name on the next run of the same lane — clear it
# first (no-op when absent). LANE_UNIT already includes .scope (same name
# systemd-run --scope registers). LANE_UNIT_SV carries its own .scope suffix
# for the same reason: a killed RuntimeMaxSec-only self-verify leaves that
# name failed and the next same-lane dispatch is refused the unit (rc charged
# to TEST_CMD) [RES-01].
systemctl --user reset-failed ${LANE_UNIT} 2>/dev/null || true
systemctl --user reset-failed ${LANE_UNIT_SV} 2>/dev/null || true
systemctl --user reset-failed ${LEGACY_LANE_UNIT} 2>/dev/null || true
systemctl --user reset-failed ${LEGACY_LANE_UNIT_SV} 2>/dev/null || true
# Residual wall-clock bound on grok (RES-02): read the clock once per dispatch
# and thread the resolved integer into both RUNNER (RuntimeMaxSec) and TW.
# Setup overrun defers (exit 75) before starting grok. TW assembly is pure;
# residual check stays inline (not inside the subshell).
# _vm_span_elapsed: rebased to _RP_ENTRY (not _RP_START) so partial vm_span
# covers in-VM admission. Used only for the phases record / partial write — not
# a host-inclusive setup subtotal (that key is adapter-only) [REV0192R3-D2].
_vm_span_elapsed=\$(( \$(date +%s) - _RP_ENTRY ))
# CARD-09: reserve harvest time outside the agent process bound.
_harvest_reserve_s=\$(( (${TIMEOUT} + 19) / 20 ))
if [ "\$_harvest_reserve_s" -lt 60 ]; then _harvest_reserve_s=60; fi
_setup_residual_s=\$(( _BOUND_DEADLINE - \$(date +%s) ))
_reserve_cap_s=\$(( _setup_residual_s / 4 ))
if [ "\$_reserve_cap_s" -lt 0 ]; then _reserve_cap_s=0; fi
if [ "\$_harvest_reserve_s" -gt "\$_reserve_cap_s" ]; then _harvest_reserve_s=\$_reserve_cap_s; fi
_AGENT_DEADLINE=\$(( _BOUND_DEADLINE - _harvest_reserve_s ))
echo "remote_agent: timeout_budget_receipt requested_seconds=${TIMEOUT} effective_seconds=${TIMEOUT} harvest_reserve_seconds=\$_harvest_reserve_s" >&2
_grok_budget=\$(( _setup_residual_s - _harvest_reserve_s ))
if [ "\$_grok_budget" -le 0 ]; then
    # Post-materialize partial: expensive cold sample must be written + fetched.
    # Emit BEFORE the diagnostic echo so exit-75 producer walkers still resolve
    # the human-readable message adjacent to exit 75 [REV0192S1-AB-01].
    _PHASES_PARTIAL=1
    _emit_phases_record || true
    echo "remote_agent: budget_refusal reason=setup_exhausted residual_seconds=\$_setup_residual_s reserve_seconds=\$_harvest_reserve_s" >&2
    exit 75
fi
# BEGIN PHASE_BUDGET_MODEL_CLOCK
# Separate model-process clock (internal): AgentSpec.lane_timeout_s
# bounds ONLY the model process. _BOUND_DEADLINE stays the whole body, so a
# model ending inside its own clock leaves the remainder to self-verify and
# harvest; one that overruns is stopped by the same RUNNER/TW bound at its own
# clock and classified exit 8. The host refused a malformed clock (exit 7);
# Re-check the shipped copy against the host schema: required clocks must be
# positive, and every clock must be an integer in 0..86400. A lost or invalid
# required clock must not widen the model window to the whole-body budget.
_model_clock_requested_s=\$(python3 -c 'import json, sys
spec = json.load(open(sys.argv[1], encoding="utf-8"))
requires_timeout = bool(spec.get("requires_timeout"))
v = spec.get("lane_timeout_s")
if v is None and not requires_timeout:
    v = 0
print(v if isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 86400 and not (requires_timeout and v == 0) else "invalid")' "\$ROOT/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json" 2>/dev/null) || _model_clock_requested_s=""
case "\$_model_clock_requested_s" in
    ''|*[!0-9]*)
        echo "remote_agent: policy refused: model clock unreadable from shipped agent spec (lane_timeout_s)" >&2
        exit 7
        ;;
esac
_model_clock_mode=whole_body
if [ "\$_model_clock_requested_s" -gt 0 ] && [ "\$_model_clock_requested_s" -lt "\$_grok_budget" ]; then
    _grok_budget=\$_model_clock_requested_s
    _AGENT_DEADLINE=\$(( \$(date +%s) + _grok_budget ))
    _model_clock_mode=separate
fi
echo "remote_agent: phase_budget_receipt requested_body_seconds=${TIMEOUT} effective_body_seconds=${GROK_TIMEOUT} remaining_body_seconds=\$_setup_residual_s requested_model_seconds=\$_model_clock_requested_s effective_model_seconds=\$_grok_budget harvest_reserve_seconds=\$_harvest_reserve_s model_clock=\$_model_clock_mode" >&2
# END PHASE_BUDGET_MODEL_CLOCK
# Single assembly site: whole RUNNER string from the bound ladder [TEST-15].
# RuntimeMaxSec uses the residual already checked above (not a second clock).
# A straggler keeps ${LANE_UNIT} loaded after the agent exits. Do not stop or
# kill it: the process can be shared with other lanes [RES-20][RES-13]. While
# this shell holds the lane lock, fall back to a free reuse scope [OBS-05].
${_LANE_SCOPE_CGROUP_MEMBERS_SRC}
_lane_scope_show() {
    _lss_unit="\${1:-}"
    command -v timeout >/dev/null 2>&1 || return 125
    timeout -k 1 5 systemctl --user show --no-pager --property=LoadState --property=ActiveState --property=SubState --property=TasksCurrent --property=ControlGroup "\$_lss_unit" 2>/dev/null
}
_lane_scope_prop() {
    printf '%s\n' "\$2" | sed -n "s/^\$1=//p" | head -n1
}
_lane_scope_has_prop() {
    _lsh_key="\${1:-}"
    _lsh_props="\${2:-}"
    case "
\$_lsh_props
" in
        *"
\${_lsh_key}="*) return 0 ;;
    esac
    return 1
}
_lane_scope_props_complete() {
    _lpc_props="\${1:-}"
    for _lpc_key in LoadState ActiveState SubState TasksCurrent ControlGroup; do
        _lane_scope_has_prop "\$_lpc_key" "\$_lpc_props" || return 1
    done
    return 0
}
_lane_scope_probe_reason() {
    _lpr_status="\${1:-0}"
    if [ "\$_lpr_status" -eq 125 ]; then
        printf '%s' timeout_unavailable
    elif [ "\$_lpr_status" -eq 124 ] || [ "\$_lpr_status" -eq 137 ]; then
        printf '%s' probe_timeout
    elif [ "\$_lpr_status" -ne 0 ]; then
        printf '%s' probe_failed
    else
        printf '%s' probe_partial
    fi
}
_lane_scope_defer_unknown() {
    echo "remote_agent: lane_scope_busy unit=\${1:-unknown} reason=probe_unknown probe_reason=\${2:-probe_partial}" >&2
    exit 75
}
_lane_scope_classify_state() {
    _lsc_load="\${1:-}"
    _lsc_active="\${2:-}"
    case "\$_lsc_load:\$_lsc_active" in
        :*|*:) _LANE_SCOPE_PROBE_REASON=probe_partial; return 2 ;;
        loaded:inactive|not-found:inactive) return 1 ;;
        loaded:*) return 0 ;;
        *) _LANE_SCOPE_PROBE_REASON=load_state_unknown; return 2 ;;
    esac
}
_lane_scope_primary_state() {
    _lps_unit="\${1:-}"
    _LANE_SCOPE_STATE_PROPS="\$(_lane_scope_show "\$_lps_unit")"
    _lps_rc=\$?
    if [ "\$_lps_rc" -ne 0 ]; then
        _LANE_SCOPE_PROBE_REASON="\$(_lane_scope_probe_reason "\$_lps_rc")"
        return 2
    fi
    if ! _lane_scope_props_complete "\$_LANE_SCOPE_STATE_PROPS"; then
        _LANE_SCOPE_PROBE_REASON=probe_partial
        return 2
    fi
    _LANE_SCOPE_STATE_LOAD="\$(_lane_scope_prop LoadState "\$_LANE_SCOPE_STATE_PROPS")"
    _LANE_SCOPE_STATE_ACTIVE="\$(_lane_scope_prop ActiveState "\$_LANE_SCOPE_STATE_PROPS")"
    _LANE_SCOPE_STATE_SUB="\$(_lane_scope_prop SubState "\$_LANE_SCOPE_STATE_PROPS")"
    _LANE_SCOPE_STATE_TASKS="\$(_lane_scope_prop TasksCurrent "\$_LANE_SCOPE_STATE_PROPS")"
    _lane_scope_classify_state "\$_LANE_SCOPE_STATE_LOAD" "\$_LANE_SCOPE_STATE_ACTIVE"
}
_lane_scope_members() {
    _lsm_unit="\${1:-}"
    if [ "\$#" -ge 2 ]; then
        _lsm_props="\$2"
    else
        _lsm_props="\$(_lane_scope_show "\$_lsm_unit")"
    fi
    _lsm_cg="\$(_lane_scope_prop ControlGroup "\$_lsm_props")"
    [ -n "\$_lsm_cg" ] || return 0
    _lane_scope_cgroup_members "\$_lsm_cg"
}
_lane_scope_loaded() {
    _lsl_props="\$(_lane_scope_show "\${1:-}")"
    _lsl_rc=\$?
    if [ "\$_lsl_rc" -ne 0 ]; then
        _LANE_SCOPE_PROBE_REASON="\$(_lane_scope_probe_reason "\$_lsl_rc")"
        return 2
    fi
    if ! _lane_scope_props_complete "\$_lsl_props"; then
        _LANE_SCOPE_PROBE_REASON=probe_partial
        return 2
    fi
    _lsl_load="\$(_lane_scope_prop LoadState "\$_lsl_props")"
    _lsl_active="\$(_lane_scope_prop ActiveState "\$_lsl_props")"
    _lane_scope_classify_state "\$_lsl_load" "\$_lsl_active"
}
_lane_scope_select() {
    if [ "\${_lane_flock_available:-1}" -eq 1 ]; then
        flock -n 9 || { echo 'remote_agent: lane_scope_select without lane lock - deferring' >&2; exit 75; }
    fi
    _lane_scope_primary_unit='${LANE_UNIT}'
    _lane_scope_primary_sv_unit='${LANE_UNIT_SV}'
    _lane_scope_key='${LANE_KEY}'
    _LANE_SCOPE_PRIMARY="\$_lane_scope_primary_unit"
    _LANE_SCOPE_PRIMARY_SV="\$_lane_scope_primary_sv_unit"
    _LANE_SCOPE_UNIT_OVERRIDE=""
    _LANE_SCOPE_UNIT_SV_OVERRIDE=""
    if _lane_scope_primary_state "\$_LANE_SCOPE_PRIMARY"; then
        _sel_primary_state=busy
    else
        _sel_primary_rc=\$?
        if [ "\$_sel_primary_rc" -eq 2 ]; then
            _lane_scope_defer_unknown "\$_LANE_SCOPE_PRIMARY" "\$_LANE_SCOPE_PROBE_REASON"
        fi
        _sel_primary_state=free
    fi
    _sel_props="\$_LANE_SCOPE_STATE_PROPS"
    _sel_active="\$_LANE_SCOPE_STATE_ACTIVE"
    _sel_sub="\$_LANE_SCOPE_STATE_SUB"
    _sel_tasks="\$_LANE_SCOPE_STATE_TASKS"
    if _lane_scope_primary_state "\$_LANE_SCOPE_PRIMARY_SV"; then
        _sel_sv_state=busy
    else
        _sel_sv_rc=\$?
        if [ "\$_sel_sv_rc" -eq 2 ]; then
            _lane_scope_defer_unknown "\$_LANE_SCOPE_PRIMARY_SV" "\$_LANE_SCOPE_PROBE_REASON"
        fi
        _sel_sv_state=free
    fi
    _sel_sv_props="\$_LANE_SCOPE_STATE_PROPS"
    _sel_sv_active="\$_LANE_SCOPE_STATE_ACTIVE"
    _sel_sv_sub="\$_LANE_SCOPE_STATE_SUB"
    _sel_sv_tasks="\$_LANE_SCOPE_STATE_TASKS"
    if [ "\$_sel_primary_state" = free ] && [ "\$_sel_sv_state" = free ]; then
        return 0
    fi
    if [ "\$_sel_primary_state" = busy ]; then
        _sel_busy_unit="\$_LANE_SCOPE_PRIMARY"
        _sel_busy_props="\$_sel_props"
        _sel_busy_active="\$_sel_active"
        _sel_busy_sub="\$_sel_sub"
        _sel_busy_tasks="\$_sel_tasks"
    else
        _sel_busy_unit="\$_LANE_SCOPE_PRIMARY_SV"
        _sel_busy_props="\$_sel_sv_props"
        _sel_busy_active="\$_sel_sv_active"
        _sel_busy_sub="\$_sel_sv_sub"
        _sel_busy_tasks="\$_sel_sv_tasks"
    fi
    _sel_members="\$(_lane_scope_members "\$_sel_busy_unit" "\$_sel_busy_props")"
    echo "remote_agent: journal lane_scope_busy unit=\${_sel_busy_unit} active_state=\${_sel_busy_active} sub_state=\${_sel_busy_sub} tasks_current=\${_sel_busy_tasks} members=\${_sel_members}" >&2
    _sel_n=1
    while [ "\$_sel_n" -le 9 ]; do
        _sel_cand="workbay-lane-\${_lane_scope_key}-r\${_sel_n}.scope"
        _sel_cand_sv="workbay-lane-\${_lane_scope_key}-r\${_sel_n}-sv.scope"
        _sel_cand_state=not_found
        if _lane_scope_loaded "\$_sel_cand"; then
            _sel_cand_state=loaded
        else
            _sel_cand_rc=\$?
            if [ "\$_sel_cand_rc" -eq 2 ]; then
                _lane_scope_defer_unknown "\$_sel_cand" "\$_LANE_SCOPE_PROBE_REASON"
            fi
        fi
        if [ "\$_sel_cand_state" = loaded ]; then
            _sel_n=\$((_sel_n + 1))
            continue
        fi
        _sel_cand_sv_state=not_found
        if _lane_scope_loaded "\$_sel_cand_sv"; then
            _sel_cand_sv_state=loaded
        else
            _sel_cand_sv_rc=\$?
            if [ "\$_sel_cand_sv_rc" -eq 2 ]; then
                _lane_scope_defer_unknown "\$_sel_cand_sv" "\$_LANE_SCOPE_PROBE_REASON"
            fi
        fi
        if [ "\$_sel_cand_sv_state" = loaded ]; then
            _sel_n=\$((_sel_n + 1))
            continue
        fi
        _LANE_SCOPE_UNIT_OVERRIDE="\$_sel_cand"
        _LANE_SCOPE_UNIT_SV_OVERRIDE="\$_sel_cand_sv"
        return 0
    done
    echo "remote_agent: lane_scope_busy unit=\${_sel_busy_unit} active_state=\${_sel_busy_active} sub_state=\${_sel_busy_sub}" >&2
    exit 75
}
_lane_scope_record_owned() {
    [ -n "\${WORKBAY_REMOTE_JOB_DIR:-}" ] || return 0
    if ! printf '%s %s\n' "\$1" "\${WORKBAY_DISPATCH_NONCE:-}" "\$2" "\${WORKBAY_DISPATCH_NONCE:-}" >> "\$WORKBAY_REMOTE_JOB_DIR/owned-scopes"; then
        echo "remote_agent: journal owned_scopes_unwritable" >&2
    fi
    return 0
}
_lane_scope_bind_runner() {
    _lsb_started=\$SECONDS
    _lane_scope_select
    _lsb_budget="\${1:-}"
    if [ -n "\${_AGENT_DEADLINE:-}" ]; then
        if ! _lsb_now="\$(date +%s)"; then
            echo "remote_agent: lane_scope_busy unit=\${_LANE_SCOPE_PRIMARY} reason=agent_budget_unavailable" >&2
            exit 75
        fi
        case "\$_lsb_now:\$_AGENT_DEADLINE" in
            *[!0-9:]*|:*) echo "remote_agent: lane_scope_busy unit=\${_LANE_SCOPE_PRIMARY} reason=agent_budget_unavailable" >&2; exit 75 ;;
        esac
        _lsb_remaining=\$(( _AGENT_DEADLINE - _lsb_now ))
        case "\$_lsb_budget" in
            ''|*[!0-9]*) ;;
            *) if [ "\$_lsb_budget" -lt "\$_lsb_remaining" ]; then _lsb_remaining="\$_lsb_budget"; fi ;;
        esac
        _lsb_budget="\$_lsb_remaining"
    else
        case "\$_lsb_budget" in
            ''|*[!0-9]*) _lsb_budget=0 ;;
            *) _lsb_budget=\$(( _lsb_budget - (SECONDS - _lsb_started) )) ;;
        esac
    fi
    case "\$_lsb_budget" in
        ''|*[!0-9]*|0) echo "remote_agent: lane_scope_busy unit=\${_LANE_SCOPE_PRIMARY} reason=agent_budget_exhausted" >&2; exit 75 ;;
    esac
    set -- "\$_lsb_budget"
    RUNNER="\$(_agent_bound_runner "\${1:-}" "\${_LANE_SCOPE_UNIT_OVERRIDE:-\$_LANE_SCOPE_PRIMARY}")"
}
_lane_scope_journal_stragglers() {
    _lane_scope_primary_unit='${LANE_UNIT}'
    _js_unit="\${_LANE_SCOPE_UNIT_OVERRIDE:-\$_lane_scope_primary_unit}"
    _js_members=""
    export -f _lane_scope_cgroup_members
    if command -v timeout >/dev/null 2>&1 && command -v bash >/dev/null 2>&1; then
        _js_members=\$(WORKBAY_SCOPE_JOURNAL_UNIT="\$_js_unit" timeout 5 bash -c '
unit="\$WORKBAY_SCOPE_JOURNAL_UNIT"
props="\$(systemctl --user show --no-pager --property=ControlGroup "\$unit" 2>/dev/null)" || exit 0
cg="\$(printf "%s\n" "\$props" | sed -n "s/^ControlGroup=//p" | head -n1)"
[ -n "\$cg" ] || exit 0
_lane_scope_cgroup_members "\$cg"
') || true
    fi
    echo "remote_agent: journal lane_scope_stragglers unit=\${_js_unit} members=\${_js_members}" >&2 || true
    return 0
}
_lane_scope_bind_runner "\$_grok_budget"
_lane_scope_record_owned "\${_LANE_SCOPE_UNIT_OVERRIDE:-\$_LANE_SCOPE_PRIMARY}" "\${_LANE_SCOPE_UNIT_SV_OVERRIDE:-\$_LANE_SCOPE_PRIMARY_SV}"
TW="\$(_agent_bound_wrapper_prefix "\$_grok_budget")"
# Redirect grok stdin from /dev/null: this remote body is fed to bash -s
# on the same stdin the child inherits. A stdin-reading grok would eat the
# script tail (no-commit check + git format-patch never run; ssh returns 0
# with an empty "success" patch).
# (No backticks: unquoted <<REMOTE_EOF would fork a local bash -s at every
# dispatch while constructing this remote body [AGT-10].)
# Classify agent_failed exit status: wall-clock bound expiry → 8, else 3.
# Extractable for fragment-harness tests (implementation note GATE-M02). Both signals
# are OR'd: deadline covers wrapper/scope/ceiling; rc 124 survives a clock jump.
_classify_agent_failed_exit() {
    if [ "\$(date +%s)" -ge "\${_AGENT_DEADLINE:-\$_BOUND_DEADLINE}" ] || [ "\$_agent_rc" -eq 124 ]; then
        printf '%s\n' 8
        return 0
    fi
    printf '%s\n' 3
}
# Unsafe harvest is its own adapter-visible failure when no stronger typed
# status already exists. Timeout/quota/tier/result statuses keep precedence.
_terminate_unsafe_harvest() {
    [ "\${HARVEST_GITDIR_UNSAFE:-0}" -eq 1 ] || return 0
    _unsafe_prior_rc="\${1:-3}"
    echo "remote_agent: unsafe harvest refused: gitdir tampering detected; prior_exit=\${_unsafe_prior_rc}" >&2
    case "\$_unsafe_prior_rc" in
        5|8|9|10) exit "\$_unsafe_prior_rc" ;;
        *) exit 11 ;;
    esac
}
# Off-box self-verify capture (item 26 / implementation note S3): shared by the success path
# and the exit-3 salvage arm. FAIL-OPEN + BUDGET-BOUNDED (RES-13 / RES-02): never
# abort before the caller emits git format-patch. Guard hard deps (base64/python3);
# residual-budget skip / capture-write error leave the file absent (worker OBS-08).
_emit_off_box_selfverify() {
if [ -n '${SELFVERIFY_CMD_B64}' ] && command -v base64 >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1; then
    _sv_cmd="\$(printf '%s' '${SELFVERIFY_CMD_B64}' | base64 -d 2>/dev/null || true)"
    # Residual budget: subtract setup+grok elapsed (from _RP_START) and fetch headroom
    # from the caller's remote wall-clock so self-verify cannot push the run past the
    # local transport bound (a SIGKILL there would drop the patch). No budget -> skip.
    # Residual budget from the shared ladder deadline (one clock for every
    # consumer). Never leave both wrapper prefixes empty while _sv_cmd is set:
    # self-verify runs after the grok scope returns, so RuntimeMaxSec on that
    # unit does not cover it; an unbound TEST_CMD holds the lane flock forever [RES-02].
    _sv_tw=''
    _sv_scope_tw=''
    _sv_has_timeout=0
    if command -v timeout >/dev/null 2>&1; then _sv_has_timeout=1; fi
    if [ '${GROK_TIMEOUT}' -gt 0 ] 2>/dev/null; then
        _sv_budget=\$(( _BOUND_DEADLINE - \$(date +%s) - 15 ))
    else
        # --timeout 0: the ladder deadline is the multi-hour ceiling, far past any
        # sane verification. Keep the pre-ladder 600s cap for this arm.
        _sv_budget=\$(( _BOUND_DEADLINE - \$(date +%s) - 15 ))
        if [ "\$_sv_budget" -gt 600 ]; then _sv_budget=600; fi
    fi
    if [ "\$_sv_budget" -le 0 ]; then
        echo 'remote_agent: no residual budget for off-box self-verify — skipping capture (patch still emitted)' >&2
        _sv_cmd=''
    elif [ "\$_SCOPE_SUPPORTS_RUNTIMEMAX" = 1 ]; then
        _sv_scope_tw="systemd-run --quiet --user --scope --unit ${LANE_UNIT_SV}"
        if [ -n "\${_LANE_SCOPE_UNIT_SV_OVERRIDE:-}" ]; then
            _sv_scope_tw="\${_sv_scope_tw//${LANE_UNIT_SV}/\${_LANE_SCOPE_UNIT_SV_OVERRIDE}}"
        fi
        if [ "\${_SCOPE_OK:-0}" = 1 ]; then
            _sv_scope_tw="\$_sv_scope_tw -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA}"
        fi
        _sv_scope_tw="\$_sv_scope_tw -p RuntimeMaxSec=\$_sv_budget"
        if [ "\$_sv_has_timeout" = 1 ]; then
            _sv_tw="timeout -k 5 \$_sv_budget"
        fi
    elif [ "\$_sv_has_timeout" = 1 ]; then
        _sv_tw="timeout -k 5 \$_sv_budget"
        echo 'remote_agent: self-verify is unscoped because systemd RuntimeMaxSec scopes are unavailable' >&2
    else
        echo 'remote_agent: no process bound available for off-box self-verify — skipping capture (patch still emitted)' >&2
        _sv_cmd=''
    fi
    if [ -n "\$_sv_cmd" ]; then
        _sv_log="\$SBX/.grok-selfverify.log"
        # Re-check before PATH prepend: a concurrent LRU reap can dangling-symlink
        # \$SBX/.venv between sync and self-verify (OFFLOAD-LANEVENV-DANGLING-SYMLINK-01).
        if ! _ensure_lane_venv_symlink; then
            echo 'remote_agent: lane venv unusable at self-verify — skipping TEST_CMD (patch still emitted)' >&2
            _sv_cmd=''
        fi
    fi
    if [ -n "\$_sv_cmd" ]; then
        # 'if' guard (not a bare command): under 'set -e' a nonzero TEST_CMD would abort
        # before we capture its rc; the else arm records the real rc.
        # </dev/null: this child inherits the ssh 'bash -s' script stream on fd0 like
        # uv/grok above — a stdin-reading TEST_CMD would otherwise eat the rest of the
        # remote body (incl. git format-patch) and silently drop the committed patch.
        # implementation note S1: the caller's TEST_CMD gets a PATH that can actually resolve its
        # interpreter. A non-interactive ssh shell lacks ~/.local/bin, so 'uv' (and hence
        # 'uv run pytest') was exit 127 on 14/14 dispatches; the lane venv's bin makes a
        # bare 'pytest' resolve. VIRTUAL_ENV is exported so venv-aware tools agree.
        # This targets ONLY the caller-supplied command; the script's own uv calls already
        # use the absolute \$HOME/.local/bin/uv.
        _sv_guard_nonce="\$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
        _sv_deadline=\$(( \$(date +%s) + _sv_budget ))
        # Open capture files within the timeout, without following links or blocking
        # on command-controlled FIFOs. Validate descriptors before truncating.
        _sv_scope_started="\$SBX/.grok-selfverify.scope-started"
        _sv_run_capture() {
            ( cd "\$SBX" && PATH="\$HOME/.local/bin:\$SBX/.venv/bin:\$PATH" VIRTUAL_ENV="\$SBX/.venv" WORKBAY_ORIGIN_GUARD_NONCE="\$_sv_guard_nonce" "\$@" </dev/null 9>&- )
        }
        _sv_capture_python_code='
import os, stat, sys
try:
    for target, path in ((1, sys.argv[1]), (2, sys.argv[1] + ".stderr")):
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NONBLOCK | os.O_NOFOLLOW, 0o600)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise OSError("nonregular capture log")
        os.ftruncate(fd, 0)
        os.dup2(fd, target)
        if fd != target:
            os.close(fd)
except OSError:
    sys.exit(125)
os.execvp("bash", ["bash", "-c", sys.argv[2]])
'
        _sv_capture_test() {
            _sv_capture_mode="\$1"
            shift
            if [ "\$_sv_capture_mode" = scoped ]; then
                _sv_run_capture \$_sv_scope_tw bash -c 'printf started > "\$1"; shift; exec "\$@"' bash "\$_sv_scope_started" \$_sv_tw python3 -c "\$_sv_capture_python_code" "\$@"
            else
                _sv_run_capture \$_sv_tw python3 -c "\$_sv_capture_python_code" "\$@"
            fi
        }
        _sv_capture_not_run=0
        if [ -n "\$_sv_scope_tw" ]; then
            rm -f "\$_sv_scope_started"
            if _sv_capture_test scoped "\$_sv_log" "\$_sv_cmd"; then
                _sv_rc=0
            else
                _sv_rc=\$?
                if [ ! -e "\$_sv_scope_started" ]; then
                    if [ "\$_sv_has_timeout" = 1 ]; then
                        echo 'remote_agent: self_verify_scope_unavailable: systemd scope creation failed; retrying self-verify unscoped under timeout' >&2
                        if _sv_capture_test timeout "\$_sv_log" "\$_sv_cmd"; then
                            _sv_rc=0
                        else
                            _sv_rc=\$?
                        fi
                    else
                        echo 'remote_agent: self_verify_scope_unavailable: systemd scope creation failed and timeout is unavailable; skipping TEST_CMD' >&2
                        _sv_cmd=''
                        _sv_capture_not_run=1
                    fi
                fi
            fi
            rm -f "\$_sv_scope_started"
        else
            if _sv_capture_test timeout "\$_sv_log" "\$_sv_cmd"; then
                _sv_rc=0
            else
                _sv_rc=\$?
            fi
        fi
        if [ "\$_sv_capture_not_run" = 0 ]; then
        # A capture-write failure must never abort before format-patch.
        _sv_json_rc=0
        set +e
        SV_DEADLINE="\$_sv_deadline" SV_STDERR="\$_sv_log.stderr" SV_RC="\$_sv_rc" SV_CMD="\$_sv_cmd" SV_LOG="\$_sv_log" SV_GUARD_NONCE="\$_sv_guard_nonce" python3 - > "\$OUT_DIR/.grok-selfverify.json" 2>/dev/null <<'PYEOF'
import json, os, re, signal, stat, time
rc = int(os.environ.get("SV_RC", "1") or "1")
tail = os.environ.get("SV_TAIL", "")
# implementation note S1 [REF-01]: a bool cannot distinguish "your tests failed" from "I could not
# find an interpreter". Emit an outcome enum alongside it. 126 = found-but-not-executable,
# 127 = not-found; bash reports both for a harness fault, not a test result.
reason = ""
# Only a nonce-bearing guard diagnostic plus pytest UsageError is guard evidence.
guard_tail = os.environ.get("SV_GUARD_TAIL", "")
red_pattern = re.compile(rb"(?m)^(?:[.FE s xX]+[FE][.FE s xX]*$|[FE]$|FAILED |ERROR .* - )|\b[1-9][0-9]* (?:failed|errors?)\b")
run_pattern = re.compile(rb"test session starts|\b[1-9][0-9]* passed\b")
UI_TEST_BROWSER_FAILURE_PATTERNS = (
    (
        re.compile(rb"sandbox_host_linux\.cc.*Operation not permitted"),
        "ui_test_browser_launch_failed",
    ),
    (
        re.compile(
            rb"browserType\.launch: .*(?:Target page, context or browser has been closed|Failed to launch)"
        ),
        "ui_test_browser_launch_failed",
    ),
    (
        re.compile(rb"Failed to launch the browser process"),
        "ui_test_browser_launch_failed",
    ),
    (
        re.compile(rb"browserType\.launch: Executable doesn't exist"),
        "ui_test_capability_missing",
    ),
    (
        re.compile(rb"Looks like Playwright .* just installed or updated"),
        "ui_test_capability_missing",
    ),
    (
        re.compile(rb"Host system is missing dependencies to run browsers"),
        "ui_test_capability_missing",
    ),
)
red_evidence = bool(red_pattern.search(tail.encode()))
run_evidence = bool(run_pattern.search(tail.encode()))
budget_expired = False
log_nonregular = False
class NonregularLog(Exception):
    pass

def regular_log(path, mode):
    # O_NONBLOCK prevents a FIFO replacement race from blocking open itself.
    if not stat.S_ISREG(os.lstat(path).st_mode):
        raise NonregularLog()
    flags = os.O_RDONLY if mode == "rb" else os.O_WRONLY | os.O_APPEND
    fd = os.open(path, flags | os.O_NONBLOCK | os.O_NOFOLLOW)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise NonregularLog()
    return os.fdopen(fd, mode)

def expire_budget(*_):
    raise TimeoutError("self-verify processing budget exhausted")
try:
    deadline = float(os.environ.get("SV_DEADLINE", "0"))
    if deadline:
        signal.signal(signal.SIGALRM, expire_budget)
        remaining = deadline - time.time()
        if remaining <= 0:
            expire_budget()
        signal.setitimer(signal.ITIMER_REAL, remaining)
    if os.environ.get("SV_LOG"):
        # RVSVO3-01: fixed chunks plus overlap bound memory even without newlines.
        # RVSVO2-01: scan all evidence before retaining only the diagnostic tail.
        paths = (os.environ["SV_LOG"], os.environ.get("SV_STDERR"))
        for path in paths:
            if path and not stat.S_ISREG(os.lstat(path).st_mode):
                raise NonregularLog()
        tail_bytes = b""
        guard_bytes = b""
        for path in (os.environ["SV_LOG"], os.environ.get("SV_STDERR")):
            if not path:
                continue
            window = b""
            with regular_log(path, "rb") as log:
                while True:
                    chunk = log.read(65536)
                    if not chunk:
                        break
                    window += chunk
                    red_evidence |= bool(red_pattern.search(window))
                    run_evidence |= bool(run_pattern.search(window))
                    window = window[-256:]
                    tail_bytes = (tail_bytes + chunk)[-8000:]
                    if path == os.environ.get("SV_STDERR"):
                        guard_bytes = (guard_bytes + chunk)[-8000:]
                        with regular_log(os.environ["SV_LOG"], "ab") as combined:
                            combined.write(chunk)
        guard_tail = guard_bytes.decode("utf-8", errors="replace")
        tail = tail_bytes.decode("utf-8", errors="replace").rstrip("\n")
except TimeoutError:
    budget_expired = True
except (NonregularLog, OSError):
    log_nonregular = True
finally:
    signal.setitimer(signal.ITIMER_REAL, 0)
guard_nonce = os.environ.get("SV_GUARD_NONCE", "")
guard_line = next((ln.strip() for ln in guard_tail.splitlines()
    if guard_nonce and ln.strip().endswith("[workbay-origin:" + guard_nonce + "]")
    and re.search(r"^ERROR: workbay_\w+ loaded from .+ but cwd is .+", ln.strip())), "")
zero_line = next((ln.strip() for ln in tail.splitlines()
    if re.search(r"\b(?:collected 0 items|no tests ran)\b", ln)), "")
browser_failure = next(
    (
        (reason_code, line.strip()[:200])
        for line in tail.splitlines()
        for pattern, reason_code in UI_TEST_BROWSER_FAILURE_PATTERNS
        if pattern.search(line.encode())
    ),
    None,
)
if browser_failure:
    browser_reason, browser_line = browser_failure
    reason = f"{browser_reason}: {browser_line}"
browser_passed_evidence = bool(re.search(rb"\b[1-9][0-9]* passed\b", tail.encode()))
if log_nonregular:
    outcome = "harness_error"
    reason = "selfverify_log_nonregular"
elif budget_expired:
    outcome = "failed"
    reason = "self-verify processing budget exhausted"
elif rc != 0 and browser_failure and not browser_passed_evidence:
    outcome = "harness_error"
elif rc == 0:
    outcome = "passed"
elif red_evidence or run_evidence:
    # Fail closed on ambiguity: suite evidence vetoes every harness-fault hint.
    outcome = "failed"
elif rc == 4 and (guard_line or zero_line):
    # Adapter contract: harness_error is the supported environment-fault enum.
    outcome = "harness_error"
    reason = guard_line or zero_line
elif rc in (126, 127):
    outcome = "harness_error"
else:
    _last = next((ln for ln in reversed(tail.splitlines()) if ln.strip()), "")
    if re.search(r"^(?:[^\s:]*/)?(?:ba)?sh: (?:line \d+: )?.+: (?:command not found|No such file or directory|Permission denied)$", _last.strip()):
        outcome = "harness_error"
    else:
        outcome = "failed"
# NB "passed" is retained unchanged for backward compat; consumers migrating to the enum
# must read self_verify_outcome. The absent-capture state is "not_run" and is decided by
# the CONSUMER (this block does not run when there is no capture).
print(json.dumps({
    "command": os.environ.get("SV_CMD", ""),
    "exit_code": rc,
    "passed": outcome == "passed",
    "self_verify_outcome": outcome,
    "reason": reason,
    "output_tail": tail,
}))
PYEOF
        _sv_json_rc=\$?
        set -e
        [ "\$_sv_json_rc" -eq 0 ] || true
        if [ -f "\$_sv_log.stderr" ] || [ -L "\$_sv_log.stderr" ]; then
            rm -f "\$_sv_log.stderr" || :
        fi
        echo "remote_agent: off-box self-verify exit \$_sv_rc (patch emitted regardless)" >&2
        fi
    fi
fi
}

# Salvage committed sandbox work on failure arms (finding 14073): when HEAD has
# diverged from BASE, emit the off-box self-verify record and stream commits
# back as a patch so the operator does not lose work. Shared by result_degraded,
# result_rewrite_failed, agent_failed, the catch-all unknown-status arm, and the
# EXIT trap (OFFLOAD-EXIT1-NO-STDERR-TAIL-01). auth_failed is exempt — auth
# match fails before the agent runs.
# DURFIX-SALVAGE-EMPTY-PATCH-01: empty format-patch while HEAD != BASE is a
# loud failure (return 1 + stderr), never a silent success-looking 0-byte patch.
_SALVAGE_DONE=0
_SALVAGE_ATTEMPTED=0
_SALVAGE_COMPLETE=0
_STDERR_TAIL_DONE=0
HARVEST_GITDIR_UNSAFE=0
# The .git writable_roots grant is the FULL gitdir. Neutralize hook/config
# injection and fail-closed on object-lookup redirects before any harvest git
# (rev-parse / diff / format-patch) runs as the gate user outside seatbelt.
# Split so a new gitdir redirect is one abort call plus one focused test;
# the 13 copy-pasted abort blocks previously hid missed arms.
_harvest_tamper_abort() {
    HARVEST_GITDIR_UNSAFE=1
    _STDERR_TAIL_DONE=1
    echo 'remote_agent: gitdir tampering detected' >&2
    return 1
}
# All Git operations after the agent returns share the dispatch deadline and
# an isolated, network-disabled configuration. A poisoned metadata node must
# not outlive the lane's remaining wall-clock budget even if a structure check
# races with the eventual open(2).
_harvest_git() {
    case "\${_BOUND_DEADLINE:-}" in
        ''|*[!0-9]*)
            echo 'remote_agent: harvest deadline unavailable' >&2
            return 124
            ;;
    esac
    _harvest_now=\$(date +%s) || return 124
    _harvest_remaining=\$(( _BOUND_DEADLINE - _harvest_now ))
    if [ "\$_harvest_remaining" -le 0 ]; then
        echo 'remote_agent: harvest deadline exhausted' >&2
        return 124
    fi
    if command -v timeout >/dev/null 2>&1; then
        timeout -k 1 "\$_harvest_remaining" env -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_NO_LAZY_FETCH=1 GIT_NO_REPLACE_OBJECTS=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c protocol.http.allow=never -c protocol.https.allow=never -c protocol.git.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null "\$@"
    elif [ "\${_SCOPE_SUPPORTS_RUNTIMEMAX:-0}" -eq 1 ]; then
        systemd-run --quiet --user --scope -p RuntimeMaxSec="\$_harvest_remaining" env -u GIT_CONFIG -u GIT_CONFIG_PARAMETERS -u GIT_CONFIG_COUNT GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_NO_LAZY_FETCH=1 GIT_NO_REPLACE_OBJECTS=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c protocol.http.allow=never -c protocol.https.allow=never -c protocol.git.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null "\$@"
    else
        echo 'remote_agent: no process bound available for harvest git' >&2
        return 124
    fi
}
_harvest_gitdir_structure_checks() {
    # A file or symlink .git is a gitdir: pointer (or worse). Path checks
    # against .git/... no-op and git follows the pointer to an attacker gitdir.
    if [ -L .git ]; then
        _harvest_tamper_abort || return 1
    fi
    if [ -e .git ] && [ ! -d .git ]; then
        _harvest_tamper_abort || return 1
    fi
    # HEAD is always consumed by harvest. The index is consumed by the
    # uncommitted-work arm when present. Reject links and special files before
    # Git can block on a FIFO or follow attacker-selected metadata.
    if [ -L .git/HEAD ] || [ ! -f .git/HEAD ]; then
        _harvest_tamper_abort || return 1
    fi
    if [ -L .git/index ] || { [ -e .git/index ] && [ ! -f .git/index ]; }; then
        _harvest_tamper_abort || return 1
    fi
    if [ -L .git/packed-refs ] || { [ -e .git/packed-refs ] && [ ! -f .git/packed-refs ]; }; then
        _harvest_tamper_abort || return 1
    fi
    if [ -L .git/refs ] || { [ -e .git/refs ] && [ ! -d .git/refs ]; }; then
        _harvest_tamper_abort || return 1
    fi
    if [ -e .git/objects/info/alternates ] || [ -e .git/objects/info/http-alternates ] || [ -e .git/commondir ] || [ -e .git/info/grafts ] || [ -e .git/refs/replace ]; then
        _harvest_tamper_abort || return 1
    fi
    # A symlinked object store (or pack/info under it) lets harvest git
    # resolve an attacker-controlled store even when alternates/commondir
    # are absent. Check the link itself — do not follow it.
    if [ -L .git/objects ] || [ -L .git/objects/pack ] || [ -L .git/objects/info ]; then
        _harvest_tamper_abort || return 1
    fi
    # A child symlink under a real objects/pack/info directory (e.g. a planted
    # pack-*.pack link) also redirects harvest git at an attacker store.
    # Missing or broken find must fail-closed — an empty substitution is not
    # a clean tree ([ARCH-13]). Do not use -quit (non-GNU find rejects it).
    if ! command -v find >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    _obj_links=
    _find_rc=0
    _obj_links=\$(find .git/objects ! -type d ! -type f -print 2>/dev/null) || _find_rc=\$?
    if [ "\$_find_rc" -ne 0 ] || [ -n "\$_obj_links" ]; then
        _harvest_tamper_abort || return 1
    fi
    _ref_special=
    _find_rc=0
    if [ -d .git/refs ]; then
        _ref_special=\$(find .git/refs ! -type d ! -type f -print 2>/dev/null) || _find_rc=\$?
    fi
    if [ "\$_find_rc" -ne 0 ] || [ -n "\$_ref_special" ]; then
        _harvest_tamper_abort || return 1
    fi
    # extensions.worktreeConfig reads .git/config.worktree after .git/config;
    # a planted worktree config survives neutralization of .git/config alone.
    if [ -e .git/config.worktree ] || [ -L .git/config.worktree ]; then
        _harvest_tamper_abort || return 1
    fi
    # [ -f .git/config ] is true for a symlink-to-file; neutralization is all
    # || true, so a symlink or 0444 config can keep hooksPath/sshCommand/diff.
    if [ -L .git/config ]; then
        _harvest_tamper_abort || return 1
    fi
    if [ -e .git/config ] && { [ ! -f .git/config ] || [ ! -w .git/config ]; }; then
        _harvest_tamper_abort || return 1
    fi
    return 0
}
_harvest_gitdir_neutralize_config() {
    # Harvest git must never lazy-fetch, inherit config, or speak a network
    # protocol. Reject includes from the raw regular config before git parses
    # it: an include target may be a FIFO and block open() indefinitely.
    export GIT_NO_LAZY_FETCH=1
    if ! command -v sed >/dev/null 2>&1 || ! command -v grep >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    _pre_cfg=
    if ! _pre_cfg=\$(sed -e ':a' -e '/\\\\\$/N' -e 's/\\\\\\n//' -e 'ta' .git/config 2>/dev/null); then
        _harvest_tamper_abort || return 1
    fi
    if printf '%s\n' "\$_pre_cfg" | grep -qiE '^[[:space:]]*\[(include|includeif)([].[:space:]]|$)'; then
        _harvest_tamper_abort || return 1
    fi
    # Malformed config: git leftover-scan with an OR-true guard would fail-open.
    if ! _harvest_git config --file .git/config --name-only --list >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    # Promisor / partialclone / insteadOf / credential / protocol / fetch
    # keys are not neutralized — any presence is fail-closed. Regex is
    # lowercase because git config --name-only emits canonical keys
    # (core.hookspath) and --get-regexp is case-sensitive.
    _forb=\$(_harvest_git config --file .git/config --name-only --get-regexp '^(remote\.|extensions\.|credential\.|uploadpack\.|fetch\.|url\..*\.insteadof|protocol\..*\.allow|core\.(alternaterefscommand|gitproxy|pager|askpass)|gpg\.|alias\.|format\.|http\.)' 2>/dev/null || true)
    if [ -n "\$_forb" ]; then
        _harvest_tamper_abort || return 1
    fi
    # Neutralize includes FIRST so [include]/includeIf cannot re-introduce
    # a driver that survives the later --unset / --remove-section.
    _harvest_git config --file .git/config --remove-section include 2>/dev/null || true
    _harvest_git config --file .git/config --unset-all include.path 2>/dev/null || true
    _incif_keys=\$(_harvest_git config --file .git/config --name-only --get-regexp '^includeIf\..*\.path\$' 2>/dev/null || true)
    if [ -n "\$_incif_keys" ]; then
        printf '%s\n' "\$_incif_keys" | while IFS= read -r _ik; do
            [ -n "\$_ik" ] || continue
            _harvest_git config --file .git/config --unset-all "\$_ik" 2>/dev/null || true
        done
    fi
    _harvest_git config --file .git/config --unset-all core.fsmonitor 2>/dev/null || true
    _harvest_git config --file .git/config --unset-all core.hooksPath 2>/dev/null || true
    _harvest_git config --file .git/config --unset-all core.sshCommand 2>/dev/null || true
    _harvest_git config --file .git/config --unset-all diff.external 2>/dev/null || true
    _harvest_git config --file .git/config --remove-section diff 2>/dev/null || true
    _harvest_git config --file .git/config --remove-section filter 2>/dev/null || true
    _df_keys=\$(_harvest_git config --file .git/config --name-only --get-regexp '^(diff|filter)\.' 2>/dev/null || true)
    if [ -n "\$_df_keys" ]; then
        printf '%s\n' "\$_df_keys" | while IFS= read -r _dk; do
            [ -n "\$_dk" ] || continue
            _harvest_git config --file .git/config --unset-all "\$_dk" 2>/dev/null || true
        done
    fi
    return 0
}
_harvest_gitdir_leftover_and_raw_scans() {
    # Read the file itself — a git core.fsmonitor=false override would otherwise
    # make --get-regexp report our own hardening override as a leftover key.
    # Lowercase regex: git emits core.hookspath; camelCase missed leftovers.
    _left=\$(_harvest_git config --file .git/config --name-only --get-regexp '^(include\.|includeif\.|core\.(hookspath|sshcommand|fsmonitor|alternaterefscommand|gitproxy|pager|askpass)|diff\.|filter\.|remote\.|extensions\.|credential\.|uploadpack\.|fetch\.|url\..*\.insteadof|protocol\..*\.allow|gpg\.|alias\.|format\.|http\.)' 2>/dev/null || true)
    if [ -n "\$_left" ]; then
        _harvest_tamper_abort || return 1
    fi
    # Raw-file scan: case-insensitive + line-continuation tolerant. A
    # A split hooks-backslash + Path token (or a Remote section) that git's parser
    # rejects must still fail-closed, not skip the leftover check.
    if ! command -v sed >/dev/null 2>&1 || ! command -v grep >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    _raw_cfg=\$(sed -e ':a' -e '/\\\\\$/N' -e 's/\\\\\\n//' -e 'ta' .git/config 2>/dev/null || true)
    if printf '%s\n' "\$_raw_cfg" | grep -qiE '^[[:space:]]*\[(remote|extensions|credential|uploadpack|fetch|url|protocol|include|includeif|filter|gpg|alias|format|http)([].[:space:]]|$)' \
        || printf '%s\n' "\$_raw_cfg" | grep -qiE '^[[:space:]]*(hookspath|sshcommand|fsmonitor|alternaterefscommand|gitproxy|pager|askpass|insteadof)[[:space:]]*=' \
        || printf '%s\n' "\$_raw_cfg" | grep -qiE '^[[:space:]]*\[diff[][:space:]]'; then
        _harvest_tamper_abort || return 1
    fi
    return 0
}
_sanitize_harvest_gitdir() {
    _harvest_gitdir_structure_checks || return 1
    rm -rf .git/hooks 2>/dev/null || true
    if [ -f .git/config ]; then
        _harvest_gitdir_neutralize_config || return 1
        _harvest_gitdir_leftover_and_raw_scans || return 1
    fi
    return 0
}
# Capture everything the working tree holds above HEAD into \$1.
#
# Intent-to-add first so untracked files are visible to diff HEAD, which
# then unions worktree and index; fall back to --cached when the worktree
# diff is empty but staged content exists. Returns 0 only when the destination
# ends up non-empty, so callers can distinguish "captured something" from
# "there was nothing to capture" without stat-ing the file themselves.
# Never fatal: this is a best-effort recovery path, and losing the artifact
# must never cost the caller its committed patch [RES-06].
_capture_uncommitted_work() {
    _unc_dest="\${1:-}"
    [ -n "\$_unc_dest" ] || return 1
    # Routed through _harvest_git, not raw git: this helper is defined after
    # _harvest_git, which makes it post-agent harvest code, and every git read
    # there has to share the one harvest deadline. Raw git here is how a lane
    # that is already over budget hangs the harvest it was supposed to rescue.
    # The wrapper also strips inherited GIT_CONFIG* and denies every transport,
    # so this is strictly the stronger form of the same commands.
    _harvest_git add -N . >/dev/null 2>&1 || true
    _harvest_git diff --binary --no-textconv --no-ext-diff HEAD > "\$_unc_dest" 2>/dev/null || true
    if [ ! -s "\$_unc_dest" ]; then
        _harvest_git diff --binary --no-textconv --no-ext-diff --cached > "\$_unc_dest" 2>/dev/null || true
    fi
    [ -s "\$_unc_dest" ]
}

# Emit the working-tree remainder beside the committed patch, in OUT_DIR.
#
# Harvest is a 2x2 table over (has_commits, has_dirty). The committed arms used
# to run format-patch and stop, so the (yes, yes) cell lost its remainder by
# omission -- and silently, because a well-formed non-empty patch makes every
# downstream presence check score the lane LANDED [GRPH-27].
#
# This cannot share stdout: that stream carries mbox format-patch output which
# the host parses, and splicing a raw diff into it would corrupt the patch we
# are trying to protect. An empty capture leaves no file at all, because a
# zero-byte artifact reads as "captured, and empty" -- indistinguishable from a
# capture that failed [OBS-08].
_emit_uncommitted_remainder() {
    [ "\${_REMAINDER_EMITTED:-0}" -eq 1 ] && return 0
    [ -n "\${OUT_DIR:-}" ] || return 0
    [ -d "\$OUT_DIR" ] || return 0
    _rem_path="\$OUT_DIR/uncommitted-remainder.patch"
    if _capture_uncommitted_work "\$_rem_path"; then
        _REMAINDER_EMITTED=1
        echo "remote_agent: uncommitted remainder above HEAD captured to \$_rem_path" >&2
    else
        rm -f "\$_rem_path" 2>/dev/null || true
    fi
    return 0
}
_salvage_committed_work() {
    # '_SALVAGE_DONE' is retained as the terminal guard used by older EXIT
    # hook callers.  Attempted and complete are separate: a failed format-patch
    # must still fall through to the dirty remainder capture before the hook is
    # told not to retry the expensive/sensitive harvest [OBS-08].
    [ "\${_SALVAGE_COMPLETE:-0}" -eq 1 ] && return 0
    [ "\${_SALVAGE_DONE:-0}" -eq 1 ] && return 0
    [ -n "\${BASE:-}" ] || return 0
    [ "\${_SALVAGE_ATTEMPTED:-0}" -eq 1 ] && return 1
    _SALVAGE_ATTEMPTED=1
    if ! _sanitize_harvest_gitdir; then
        HARVEST_GITDIR_UNSAFE=1
        _STDERR_TAIL_DONE=1
        _SALVAGE_DONE=1
        echo 'remote_agent: gitdir tampering detected' >&2
        return 1
    fi
    _harvest_git rev-parse --verify HEAD >/dev/null 2>&1 || return 0
    if ! _harvest_git merge-base --is-ancestor "\$BASE" HEAD >/dev/null 2>&1; then
        if [ "\${HARVEST_GITDIR_UNSAFE:-0}" -ne 1 ]; then
            _emit_uncommitted_remainder
        fi
        _SALVAGE_DONE=1
        echo 'remote_agent: source_identity_failure=source_history_not_descendant refusing salvage outside sandbox base lineage' >&2
        return 17
    fi
    if _harvest_git diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD 2>/dev/null; then
        _emit_uncommitted_remainder
        _SALVAGE_COMPLETE=1
        _SALVAGE_DONE=1
        return 0
    fi
    _emit_off_box_selfverify
    _salv_tmp=\$(mktemp "\${TMPDIR:-/tmp}/ra-salvage.XXXXXX") || {
        echo 'remote_agent: salvage mktemp failed while HEAD != BASE' >&2
        _emit_uncommitted_remainder
        _SALVAGE_DONE=1
        return 1
    }
    if ! _harvest_git format-patch --no-textconv "\$BASE"..HEAD --stdout > "\$_salv_tmp"; then
        echo 'remote_agent: salvage format-patch failed while HEAD != BASE' >&2
        rm -f "\$_salv_tmp"
        # Committed salvage failed, but the working tree is independent evidence.
        # Keep this artifact separate from the mbox stream and best-effort it
        # before the terminal guard suppresses the EXIT-hook retry.
        _emit_uncommitted_remainder
        _SALVAGE_DONE=1
        return 1
    fi
    if [ ! -s "\$_salv_tmp" ]; then
        echo 'remote_agent: salvage format-patch empty while HEAD != BASE (commits exist but patch is 0 bytes)' >&2
        echo "remote_agent: BASE=\$BASE HEAD=\$(_harvest_git rev-parse HEAD 2>/dev/null) branch=\$(_harvest_git rev-parse --abbrev-ref HEAD 2>/dev/null)" >&2
        rm -f "\$_salv_tmp"
        _emit_uncommitted_remainder
        _SALVAGE_DONE=1
        return 1
    fi
    # (has_commits=yes, has_dirty=yes): emit the remainder too, before the
    # committed patch goes out, so both halves of the mixed cell survive.
    _SALVAGE_COMPLETE=1
    _SALVAGE_DONE=1
    _emit_uncommitted_remainder
    cat "\$_salv_tmp"
    rm -f "\$_salv_tmp"
    return 0
}
_emit_nonzero_exit_diagnostics() {
    _diag_rc="\${1:-1}"
    # Once harvest gitdir tampering was detected this process, emit stderr
    # tails only — never run git against the still-poisoned gitdir.
    if [ "\${HARVEST_GITDIR_UNSAFE:-0}" -eq 1 ]; then
        echo "remote_agent: nonzero exit \${_diag_rc} — remote diagnostics follow" >&2
        if [ -n "\${_spec_stderr:-}" ] && [ -f "\$_spec_stderr" ]; then
            echo 'remote_agent: agent stderr tail:' >&2
            tail -20 "\$_spec_stderr" >&2 || true
        fi
        if [ -n "\${SBX:-}" ] && [ -f "\$SBX/.grok-run.log" ]; then
            echo 'remote_agent: run log tail:' >&2
            tail -20 "\$SBX/.grok-run.log" >&2 || true
        fi
        _STDERR_TAIL_DONE=1
        return 0
    fi
    [ "\${_STDERR_TAIL_DONE:-0}" -eq 1 ] && return 0
    _STDERR_TAIL_DONE=1
    echo "remote_agent: nonzero exit \${_diag_rc} — remote diagnostics follow" >&2
    if [ -n "\${_spec_stderr:-}" ] && [ -f "\$_spec_stderr" ]; then
        echo 'remote_agent: agent stderr tail:' >&2
        tail -20 "\$_spec_stderr" >&2 || true
    fi
    if [ -n "\${SBX:-}" ] && [ -f "\$SBX/.grok-run.log" ]; then
        echo 'remote_agent: run log tail:' >&2
        tail -20 "\$SBX/.grok-run.log" >&2 || true
    fi
    if [ -n "\${BASE:-}" ] && _harvest_git rev-parse --verify HEAD >/dev/null 2>&1; then
        if ! _harvest_git diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD 2>/dev/null; then
            echo "remote_agent: sandbox HEAD diverges from BASE (HEAD=\$(_harvest_git rev-parse --short HEAD) BASE=\$(_harvest_git rev-parse --short "\$BASE")) — salvage should have emitted a patch" >&2
        fi
    fi
}
_remote_agent_exit_hook() {
    _ra_rc=\$?
    # Salvage + stderr on every nonzero exit after BASE exists so an unclassified
    # exit 1 cannot strand commits with a silent empty patch (EXIT1-NO-STDERR).
    # _emit_phases_record stays on EVERY exit (0192 measurement window).
    if [ "\$_ra_rc" -ne 0 ]; then
        _salvage_committed_work || true
        _emit_nonzero_exit_diagnostics "\$_ra_rc" || true
    fi
    _emit_phases_record || true
    _retain_attempt_evidence || true
    _lane_clear_live_lease
}
# Replace the post-outbox EXIT trap so salvage/diagnostics run on the way out.
# Bash EXIT traps replace (do not stack); phases emit remains first-class.
trap '_remote_agent_exit_hook' EXIT

# Historical artifact filenames (de-branding is a separate plan). Kept outside
# the agent-exec sentinels so D3 assertion 2 can pin a vendor-free executor.
export AGENT_SPEC_BRIEF_FILE="\$SBX/.brief.md"
export AGENT_SPEC_SCHEMA_FILE="\$SBX/.schema.json"
export AGENT_SPEC_SCHEMA_INLINE="\$(cat "\$SBX/.schema.json")"
export AGENT_SPEC_OUT_DIR="\$OUT_DIR"
export AGENT_SPEC_RESULT_FILE="\$OUT_DIR/.agent-result.json"
export AGENT_SPEC_STREAM_FILE="\$OUT_DIR/.agent-stream.jsonl"
export AGENT_SPEC_RUN_LOG="\$SBX/.grok-run.log"
export AGENT_SPEC_DEBUG_FILE="\$OUT_DIR/.grok-debug.log"
# >>> agent-exec (backend-neutral: no vendor literals below)
# Spec-driven executor (implementation note). Host always requires --agent-spec.
if [ '${AGENT_SPEC_ENABLED}' = 1 ]; then
    # implementation note S6: fail closed when a wall-clock bound is mandatory but
    # timeout(1) is missing on the remote host (exit 7 / policy refused).
    if [ '${AGENT_SPEC_REQUIRES_TIMEOUT}' = 1 ]; then
        if ! command -v timeout >/dev/null 2>&1; then
            echo 'remote_agent: policy refused: timeout(1) unavailable (requires_timeout)' >&2
            exit 7
        fi
    fi
    # path_prepend: absolute stays; relative is under \$HOME
    _pp='${AGENT_SPEC_PATH_PREPEND:-}'
    if [ -n "\$_pp" ]; then
        _ifs="\$IFS"; IFS=:
        for _seg in \$_pp; do
            [ -n "\$_seg" ] || continue
            case "\$_seg" in
                /*) export PATH="\$_seg:\$PATH" ;;
                *) export PATH="\$HOME/\$_seg:\$PATH" ;;
            esac
        done
        IFS="\$_ifs"
    fi
    # S3-M01: argv sidecar must be non-empty, end in NUL, and have no residual.
    # Empty argv list is schema-permitted (writer emits a lone trailing NUL).
    _argv_file="\$ROOT/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.argv"
    if [ ! -s "\$_argv_file" ]; then
        echo 'remote_agent: argv sidecar empty' >&2
        exit 7
    fi
    _argv_last=\$(tail -c 1 "\$_argv_file" | od -An -tx1 | tr -d ' \\n')
    if [ "\$_argv_last" != "00" ]; then
        echo 'remote_agent: argv sidecar missing trailing NUL' >&2
        exit 7
    fi
    AGENT_ARGV=()
    while true; do
        _el=""
        if IFS= read -r -d '' _el; then
            AGENT_ARGV+=("\$_el")
        else
            # EOF without delimiter and residual bytes → truncated final element.
            if [ -n "\$_el" ]; then
                echo 'remote_agent: argv sidecar truncated (unterminated final element)' >&2
                exit 7
            fi
            break
        fi
    done < "\$_argv_file"
    _agent_spec_resolve_argv
    _spec_stdin='${AGENT_SPEC_STDIN}'
    _spec_stdout='${AGENT_SPEC_STDOUT}'
    _spec_stderr='${AGENT_SPEC_STDERR}'
    case "\$_spec_stdin" in
        '{brief_file}') _spec_stdin="\$AGENT_SPEC_BRIEF_FILE" ;;
        '{schema_file}') _spec_stdin="\$AGENT_SPEC_SCHEMA_FILE" ;;
        '{result_file}') _spec_stdin="\$AGENT_SPEC_RESULT_FILE" ;;
        '{stream_file}') _spec_stdin="\$AGENT_SPEC_STREAM_FILE" ;;
        '{run_log}') _spec_stdin="\$AGENT_SPEC_RUN_LOG" ;;
        '{debug_file}') _spec_stdin="\$AGENT_SPEC_DEBUG_FILE" ;;
        '{out_dir}') _spec_stdin="\$AGENT_SPEC_OUT_DIR" ;;
    esac
    case "\$_spec_stdout" in
        '{brief_file}') _spec_stdout="\$AGENT_SPEC_BRIEF_FILE" ;;
        '{schema_file}') _spec_stdout="\$AGENT_SPEC_SCHEMA_FILE" ;;
        '{result_file}') _spec_stdout="\$AGENT_SPEC_RESULT_FILE" ;;
        '{stream_file}') _spec_stdout="\$AGENT_SPEC_STREAM_FILE" ;;
        '{run_log}') _spec_stdout="\$AGENT_SPEC_RUN_LOG" ;;
        '{debug_file}') _spec_stdout="\$AGENT_SPEC_DEBUG_FILE" ;;
        '{out_dir}') _spec_stdout="\$AGENT_SPEC_OUT_DIR" ;;
    esac
    case "\$_spec_stderr" in
        '{brief_file}') _spec_stderr="\$AGENT_SPEC_BRIEF_FILE" ;;
        '{schema_file}') _spec_stderr="\$AGENT_SPEC_SCHEMA_FILE" ;;
        '{result_file}') _spec_stderr="\$AGENT_SPEC_RESULT_FILE" ;;
        '{stream_file}') _spec_stderr="\$AGENT_SPEC_STREAM_FILE" ;;
        '{run_log}') _spec_stderr="\$AGENT_SPEC_RUN_LOG" ;;
        '{debug_file}') _spec_stderr="\$AGENT_SPEC_DEBUG_FILE" ;;
        '{out_dir}') _spec_stderr="\$AGENT_SPEC_OUT_DIR" ;;
    esac
    # Reject unresolved '{...}' redirect tokens (same contract as argv) [WEB-02].
    for _redir in "\$_spec_stdin" "\$_spec_stdout" "\$_spec_stderr"; do
        case "\$_redir" in
            *'{'*|*'}'*)
                echo "remote_agent: invalid placeholder in redirect: \${_redir}" >&2
                exit 7
                ;;
        esac
    done
    # close_fds: S2 requires 9; hardcoded redirect (no dynamic redirection).
    case ' ${AGENT_SPEC_CLOSE_FDS} ' in
        *' 9 '*) : ;;
        *) echo 'remote_agent: close_fds must include 9' >&2; exit 7 ;;
    esac
    # BEGIN LANE_SCRATCH_ROOT
    # Per-lane scratch root for the AGENT only. It is exported as TMPDIR/TMP/TEMP
    # inside the AGENT_SPEC_ENV_LAUNCH subshell; the runner's own mktemp calls keep
    # the shared VM /tmp. The /tmp reaper arm reclaims the tree once the stamped
    # owner pid is dead and the tree is idle. Nothing in this block deletes
    # anything: reclaim stays with that reaper [RES-07][TEAM-04]. A refusal
    # degrades loudly and the agent launches exactly as before [AGT-10].
    _lane_scratch_base=/tmp
    _lane_scratch_root="\$_lane_scratch_base/workbay-run-${LANE_KEY}"
    _lane_scratch_me="\$(id -u 2>/dev/null || true)"
    _lane_scratch_refuse=
    if [ -L "\$_lane_scratch_root" ]; then
        _lane_scratch_refuse=symlink
    elif [ -e "\$_lane_scratch_root" ] && [ ! -d "\$_lane_scratch_root" ]; then
        _lane_scratch_refuse=not_dir
    elif [ ! -d "\$_lane_scratch_root" ] && ! mkdir -m 0700 "\$_lane_scratch_root" 2>/dev/null; then
        _lane_scratch_refuse=mkdir_failed
    else
        # Empty or non-numeric output means the owner is unreadable, not foreign:
        # foreign_owner is reserved for a numeric uid that differs from ours.
        _lane_scratch_uid="\$(stat -c %u -- "\$_lane_scratch_root" 2>/dev/null || stat -f %u "\$_lane_scratch_root" 2>/dev/null || true)"
        case "\$_lane_scratch_uid" in ''|*[!0-9]*) _lane_scratch_refuse=owner_unreadable ;; esac
        case "\$_lane_scratch_me" in ''|*[!0-9]*) _lane_scratch_refuse=owner_unreadable ;; esac
        if [ -z "\$_lane_scratch_refuse" ] && [ "\$_lane_scratch_uid" != "\$_lane_scratch_me" ]; then
            _lane_scratch_refuse=foreign_owner
        fi
    fi
    if [ -z "\$_lane_scratch_refuse" ]; then
        # A root that is group- or world-writable is refused, never chmodded. A
        # root this launch just made with mkdir -m 0700 passes this check.
        _lane_scratch_mode="\$(stat -c %a -- "\$_lane_scratch_root" 2>/dev/null || stat -f %Lp "\$_lane_scratch_root" 2>/dev/null || true)"
        case "\$_lane_scratch_mode" in
            ''|*[!0-7]*) _lane_scratch_refuse=mode_unreadable ;;
            *) [ \$(( 0\$_lane_scratch_mode & 022 )) -eq 0 ] || _lane_scratch_refuse=unsafe_mode ;;
        esac
    fi
    if [ -z "\$_lane_scratch_refuse" ]; then
        # Hold the root's own .lock for the whole agent run. This body shell keeps
        # fd 4 until it exits, and the agent is exec'd with 4>&-. The /tmp reaper
        # takes the same file non-blocking and reads contention as live. fd 4 is
        # used nowhere else in this file; the reaper and lane-lock code use 5-9.
        # Contention is refused, never waited on or stolen [RES-21].
        _lane_scratch_lock="\$_lane_scratch_root/.lock"
        if [ -L "\$_lane_scratch_lock" ] || { [ -e "\$_lane_scratch_lock" ] && [ ! -f "\$_lane_scratch_lock" ]; }; then
            _lane_scratch_refuse=lock_unusable
        elif ! command -v flock >/dev/null 2>&1; then
            _lane_scratch_refuse=lock_unavailable
        elif ! exec 4>>"\$_lane_scratch_lock"; then
            _lane_scratch_refuse=lock_unusable
        else
            _lane_scratch_flock_rc=0
            flock -n -E 75 4 || _lane_scratch_flock_rc=\$?
            case "\$_lane_scratch_flock_rc" in
                0) ;;
                75) _lane_scratch_refuse=lock_held ;;
                *) _lane_scratch_refuse=lock_unavailable ;;
            esac
        fi
    fi
    if [ -z "\$_lane_scratch_refuse" ]; then
        # Owner stamp, rewritten on every launch so a re-dispatched lane reclaims
        # its own stale marker. Temp file in the same directory, then mv -f. A
        # failed write leaves only our own temp name; nothing is removed.
        # The temp names are predictable and the agent can plant at them, so each
        # is removed, then created with noclobber (set -C in a subshell). A planted
        # entry makes the create fail instead of being followed and truncated
        # [SEC-01]. A failed rm is ignored: the noclobber create is the check.
        _lane_scratch_pid=\$\$
        if ! { rm -f -- "\$_lane_scratch_root/.workbay-scratch.tmp.\$_lane_scratch_pid" 2>/dev/null || true
            ( set -C; printf 'lane_key=${LANE_KEY}\npid=%s\n' "\$_lane_scratch_pid" >"\$_lane_scratch_root/.workbay-scratch.tmp.\$_lane_scratch_pid" ) \
            && mv -f "\$_lane_scratch_root/.workbay-scratch.tmp.\$_lane_scratch_pid" "\$_lane_scratch_root/.workbay-scratch" \
            && { rm -f -- "\$_lane_scratch_root/.workbay-owner.pid.tmp.\$_lane_scratch_pid" 2>/dev/null || true; } \
            && ( set -C; printf '%s\n' "\$_lane_scratch_pid" >"\$_lane_scratch_root/.workbay-owner.pid.tmp.\$_lane_scratch_pid" ) \
            && mv -f "\$_lane_scratch_root/.workbay-owner.pid.tmp.\$_lane_scratch_pid" "\$_lane_scratch_root/.workbay-owner.pid"; }; then
            _lane_scratch_refuse=stamp_failed
        fi
    fi
    if [ -n "\$_lane_scratch_refuse" ]; then
        exec 4>&-
        echo "remote_agent: lane_scratch_unavailable reason=\$_lane_scratch_refuse path=\$_lane_scratch_root" >&2
        unset _lane_scratch_root
    fi
    # END LANE_SCRATCH_ROOT
    # Launch: env_file and env apply only in the agent subshell (D6); credentials
    # never leak into the D9 post-classifier. exec remains the sole agent launch.
    # Capture rc from the else branch of a non-negated if: under POSIX an if
    # whose condition is a negated command always leaves rc at 0 (HARM-H01).
    # agent_launch: last pre-agent phase end → agent process start.
    _AGENT_START_TS=\$(date +%s)
    if [ -n "\${_AGENT_LAUNCH_OPEN_TS:-}" ]; then
        _phase_record agent_launch "\$_AGENT_LAUNCH_OPEN_TS" "\$_AGENT_START_TS"
    fi
    # BEGIN MODEL_PROCESS_START_BOUNDARY
    # RUNNER/TW execute this boundary helper. Therefore neither wrapper can
    # fail before launch while leaving a served-process receipt behind. The
    # helper resolves the real binary, durably stamps the receipt immediately
    # before exec, and retracts that stamp if exec itself is refused.
    _model_start_boundary="\$OUT_DIR/.workbay-model-process-boundary.py"
    cat > "\$_model_start_boundary" <<'PYMODELSTART'
import json
import os
import shutil
import sys
from pathlib import Path


def write_receipt(path, payload):
    tmp = path.with_name(path.name + ".start.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


receipt_path = Path(sys.argv[1])
agent = sys.argv[2]
agent_argv = sys.argv[2:]
try:
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    resolved_agent = shutil.which(agent)
    if not isinstance(payload, dict) or not resolved_agent:
        raise ValueError("agent binary unavailable after environment setup")
    payload["model_process_started"] = True
    write_receipt(receipt_path, payload)
except (OSError, ValueError, json.JSONDecodeError) as exc:
    print(f"remote_agent: model process boundary refused: {exc}", file=sys.stderr)
    raise SystemExit(127) from exc

try:
    os.execv(resolved_agent, agent_argv)
except OSError as exc:
    payload.pop("model_process_started", None)
    try:
        write_receipt(receipt_path, payload)
    except OSError:
        pass
    print(f"remote_agent: agent exec failed: {exc}", file=sys.stderr)
    raise SystemExit(127) from exc
PYMODELSTART
    # END MODEL_PROCESS_START_BOUNDARY
    _agent_rc=0
    if (
        # BEGIN AGENT_SPEC_ENV_LAUNCH
        # The emitted fragment's . "\$_envf" source command runs only in this subshell.
${AGENT_SPEC_ENV_FILE}
        # SBXVENV-01: sandbox venv environment for the AGENT's own shell.
        # Scoped to this subshell so the host-side uv keeps its own cache.
        # PATH/VIRTUAL_ENV mirror the off-box self-verify arm -- the
        # \$SBX/.venv symlink target outside \$SBX is READABLE in-sandbox
        # (probed), so a bare 'pytest'/'python' resolves to the provisioned
        # lane venv instead of falling through to a pyenv shim. Without this
        # the agent had no lane interpreter to find, which is why it reached
        # for 'uv sync' at all.
        export UV_CACHE_DIR="\$SBX_UV_CACHE"
        export PATH="\$SBX/.venv/bin:\$PATH"
        export VIRTUAL_ENV="\$SBX/.venv"
        # Agent-only temp dir: the lane's own scratch root, never the shared VM
        # /tmp. Unset when the root was refused, so nothing is exported.
        if [ -n "\${_lane_scratch_root:-}" ]; then
            export TMPDIR="\$_lane_scratch_root"
            export TMP="\$_lane_scratch_root"
            export TEMP="\$_lane_scratch_root"
        fi
        # END AGENT_SPEC_ENV_LAUNCH
        # fd 4 (the root's .lock) stays with this body shell; the agent never inherits it.
        exec \$RUNNER \$TW python3 "\$_model_start_boundary" \
                "\$OUT_DIR/.workbay-source-provenance.json" \
                '${AGENT_SPEC_BIN}' "\${AGENT_ARGV[@]}" \
                >"\$_spec_stdout" 2>"\$_spec_stderr" <"\$_spec_stdin" 4>&- 9>&-
    ); then
        _agent_rc=0
    else
        _agent_rc=\$?
    fi
    _AGENT_END_TS=\$(date +%s)
    _phase_record agent_turn "\$_AGENT_START_TS" "\$_AGENT_END_TS"
    # Best effort [RES-13][OBS-05]: name leftover scope members. Never changes
    # the agent exit code. timeout 5 bounds the probe.
    _lane_scope_journal_stragglers || true
    # Post-classify from AgentSpec auth_match + result shape (implementation note D7/D9).
    # Patterns/streams live in the JSON spec — never as vendor literals here.
    _spec_json="\$ROOT/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json"
    _post=\$(python3 - "\$_spec_json" "\$_spec_stdout" "\$_spec_stderr" "\$_agent_rc" <<'PY'
import json
import re
import sys
from pathlib import Path

spec_path, stdout_path, stderr_path, rc_s = sys.argv[1:5]
agent_rc = int(rc_s)
spec = json.loads(Path(spec_path).read_text(encoding="utf-8"))
auth = spec.get("auth_match") or {}
if not isinstance(auth, dict):
    auth = {}
rate_limit = spec.get("rate_limit_match") or {}
if not isinstance(rate_limit, dict):
    rate_limit = {}
service_tier_warning = spec.get("service_tier_warning_match") or {}
if not isinstance(service_tier_warning, dict):
    service_tier_warning = {}
patterns = [str(p) for p in (auth.get("patterns") or []) if str(p)]
exit_codes = {int(x) for x in (auth.get("exit_codes") or [])}
rate_patterns = [str(p) for p in (rate_limit.get("patterns") or []) if str(p)]
rate_exit_codes = {int(x) for x in (rate_limit.get("exit_codes") or [])}
service_tier_warning_patterns = [str(p) for p in (service_tier_warning.get("patterns") or []) if str(p)]

def _auth_stdout(text):
    # internal: JSONL tool output is data, not the agent's error channel.
    errors = []
    for line in text.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            errors.append(line)
            continue
        if not isinstance(event, dict):
            continue
        if event.get("type") == "error":
            errors.append(str(event.get("message", "")))
        elif event.get("type") == "turn.failed":
            errors.append(json.dumps(event.get("error", ""), ensure_ascii=False))
        else:
            item = event.get("item")
            if isinstance(item, dict) and item.get("type") == "error":
                errors.append(str(item.get("message", "")))
    return "\n".join(errors)

# The host scans stderr with an unanchored key:value regex
# (remote_exec._RATE_LIMIT_RESET_RE), so provider prose must never reach stderr.
# An admitted record keeps fixed fields only. Its prose stays in
# claude_rate_limit_results and is read solely for the UTC time-of-day parse.
def _quota_limit_word(result):
    match = re.search(r"(?i)\b(weekly|session|daily|monthly) limit\b", result) if isinstance(result, str) else None
    return match.group(1).lower() if match else None

def _rate_limit_stdout(text):
    # internal: stdout is a JSONL event stream, not a provider error
    # channel. Only provider error/turn.failed events, error-typed completed
    # items, or a response event carrying HTTP 429 are rate-limit evidence.
    # Ordinary worker output may quote a diagnostic fixture verbatim.
    def _is_429(value):
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return value == 429
        if isinstance(value, str):
            parts = value.strip().split(None, 1)
            return bool(parts) and parts[0] == "429"
        return False

    def _response_is_429(event):
        event_type = event.get("type")
        if not isinstance(event_type, str) or not event_type.startswith("response"):
            return False
        pending = [event]
        while pending:
            current = pending.pop()
            if not isinstance(current, dict):
                continue
            for key in ("status_code", "http_status", "http_status_code", "status"):
                if _is_429(current.get(key)):
                    return True
            for key in ("response", "error"):
                nested = current.get(key)
                if isinstance(nested, dict):
                    pending.append(nested)
        return False

    evidence = []
    for line in text.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            # Preserve an explicit wire-level HTTP response while excluding
            # free-form stdout such as echoing '429 Too Many Requests'.
            stripped = line.lstrip()
            if stripped.startswith(("HTTP/1.0 429", "HTTP/1.1 429", "HTTP/2 429", "HTTP/3 429")):
                evidence.append(line)
            continue
        if not isinstance(event, dict):
            continue
        event_type = event.get("type")
        item = event.get("item")
        wrapped_error = event_type == "item.completed" and isinstance(item, dict) and item.get("type") == "error"
        if event_type in {"error", "turn.failed"} or wrapped_error or _response_is_429(event):
            evidence.append(line)
        elif event_type == "result" and event.get("is_error") is True:
            api_error_status = event.get("api_error_status")
            if isinstance(api_error_status, int) and not isinstance(api_error_status, bool) and api_error_status == 429:
                result = event.get("result")
                if isinstance(result, str):
                    claude_rate_limit_results.append(result)
                evidence.append(
                    json.dumps(
                        {
                            "type": event_type,
                            "is_error": True,
                            "api_error_status": api_error_status,
                            "limit": _quota_limit_word(result),
                            "result_chars": len(result) if isinstance(result, str) else None,
                        },
                        separators=(",", ":"),
                    )
                )
    return "\n".join(evidence)

claude_rate_limit_results = []
rate_limit_reset_sources = []

def _stream_blob(match, *, auth_only=False, rate_only=False):
    streams = match.get("streams")
    if streams is None and match.get("stream"):
        streams = [match.get("stream")]
    if not isinstance(streams, list):
        streams = []
    stream_text = []
    for name in streams:
        path = stdout_path if name == "stdout" else stderr_path if name == "stderr" else ""
        if not path:
            continue
        try:
            text = Path(path).read_text(encoding="utf-8", errors="replace")
            if auth_only and name == "stdout":
                text = _auth_stdout(text)
            elif rate_only and name == "stdout":
                text = _rate_limit_stdout(text)
                rate_limit_reset_sources.append(text)
            elif rate_only:
                rate_limit_reset_sources.append(text)
            stream_text.append(text)
        except OSError as exc:
            print(f"remote_agent: classifier stream unreadable ({name}): {exc}", file=sys.stderr)
    return "\n".join(stream_text)

blob = _stream_blob(auth, auth_only=True)
rate_blob = _stream_blob(rate_limit, rate_only=True)
rate_reset_blob = "\n".join(rate_limit_reset_sources)
# auth_only: stderr raw + stdout error records only. Worker narration that
# merely quotes a tier string is data, not a provider tier warning.
service_tier_warning_blob = _stream_blob(service_tier_warning, auth_only=True)
# precedence: exit_codes_then_patterns (implementation note D6)
if agent_rc in exit_codes:
    print("auth_failed")
    raise SystemExit(0)
if agent_rc != 0 and (agent_rc in rate_exit_codes or (rate_patterns and any(p in rate_blob for p in rate_patterns))):
    matching_line = next(
        (line for line in rate_blob.splitlines() if any(pattern in line for pattern in rate_patterns)),
        f"exit code {agent_rc}",
    )
    print(f"remote_agent: rate_limited (upstream quota): {matching_line[:200]}", file=sys.stderr)
    reset_match = re.search(
        r'(?i)(?:x-ratelimit-reset|retry-after|reset)["\s]*[:=]["\s]*([^",}\s]+)',
        rate_reset_blob,
    )
    if reset_match:
        print(f"remote_agent: rate_limit_reset={reset_match.group(1)}", file=sys.stderr)
    else:
        quota_result = next(
            (
                result
                for result in claude_rate_limit_results
                if isinstance(result, str)
            ),
            None,
        )
        if quota_result is not None:
            quota_reset_match = re.search(
                r"(?i)\bresets (\d{1,2})(?::(\d{2}))?(am|pm) \(UTC\)",
                quota_result,
            )
            if quota_reset_match is not None:
                from datetime import datetime, timedelta, timezone

                hour = int(quota_reset_match.group(1))
                minute = int(quota_reset_match.group(2) or "0")
                if 1 <= hour <= 12 and 0 <= minute <= 59:
                    suffix = quota_reset_match.group(3).lower()
                    hour_24 = (hour % 12) + (12 if suffix == "pm" else 0)
                    # The provider text gives a time of day and no date, so the next UTC
                    # occurrence strictly after now is the only reading that invents no date.
                    now_utc = datetime.now(timezone.utc)
                    reset_at = now_utc.replace(hour=hour_24, minute=minute, second=0, microsecond=0)
                    if reset_at <= now_utc:
                        reset_at += timedelta(days=1)
                    print(f"remote_agent: rate_limit_reset={int(reset_at.timestamp() * 1000)}", file=sys.stderr)
                else:
                    print(
                        f"remote_agent: quota_reset_text_unparsed limit={_quota_limit_word(quota_result) or 'unknown'} "
                        f"result_chars={len(quota_result)}",
                        file=sys.stderr,
                    )
            else:
                print(
                    f"remote_agent: quota_reset_text_unparsed limit={_quota_limit_word(quota_result) or 'unknown'} "
                    f"result_chars={len(quota_result)}",
                    file=sys.stderr,
                )
    print("rate_limited")
    raise SystemExit(0)
# Shared with the result-shape fallback below (_result_text.py source of truth).
_RESULTISH_KEYS = frozenset(
    {
        "handoff_action",
        "findings",
        "summary",
        "tests_run",
        "blockers",
        "details",
        "merge_ready",
        "changed_files",
    }
)

def _recover_result(raw):
    # One recovery policy for auth evidence and normal result classification.
    # Pure parsing: artifact promotion/degradation belongs to the ok path.
    def _decode_object_stream(buf: str):
        # Remote agent emits ONE JSON object per turn; result.json may be a
        # concatenated run {...}{...}{...}. json.loads raises Extra data and
        # used to false-degrade complete work
        # (OFFLOAD-RESULT-UNPARSEABLE-HIDES-A-COMPLETE-TURN-PATCH-01).
        # Semantic: last complete object with handoff_action (or findings list)
        # is authoritative; do not merge across objects.
        # Call only when whole-document json.loads failed — never to scavenge
        # nested objects out of a well-formed non-object document [AGT-10].
        decoder = json.JSONDecoder()
        objs = []
        i = 0
        n = len(buf)
        while i < n:
            while i < n and buf[i].isspace():
                i += 1
            if i >= n:
                break
            if buf[i] != "{":
                nxt = buf.find("{", i)
                if nxt < 0:
                    break
                i = nxt
            try:
                obj, end = decoder.raw_decode(buf, i)
            except json.JSONDecodeError:
                i += 1
                continue
            if isinstance(obj, dict):
                objs.append(obj)
            i = end if end > i else i + 1
        return objs

    payload = None
    try:
        loaded = json.loads(raw)
        if isinstance(loaded, dict):
            payload = loaded
        # Well-formed non-dict (array/string/number/null): shape error — leave
        # payload None and do NOT enter salvage (would lift nested objects).
    except json.JSONDecodeError:
        # Salvage runs ONLY when whole-document parse failed (Extra data /
        # truncated / multi-object streams), not on successful non-dict parse.
        loaded = None
        stream = _decode_object_stream(raw)
        # Prefer last handoff-shaped, else last findings-shaped, else last dict.
        # Three ordered reverse passes (not a single OR scan): a later findings
        # object must not beat an earlier handoff object. Handoff shape requires
        # a known action value (orchestrator predicate parity), not key presence.
        _KNOWN_HANDOFF_ACTIONS = frozenset({"merge_ready", "needs_guidance"})
        for cand in reversed(stream):
            action = cand.get("handoff_action")
            if isinstance(action, str) and action in _KNOWN_HANDOFF_ACTIONS:
                payload = cand
                break
        if payload is None:
            for cand in reversed(stream):
                if isinstance(cand.get("findings"), list):
                    payload = cand
                    break
        if payload is None and stream:
            payload = stream[-1]
    if payload is None:
        return None, loaded, None

    # Shape + recovery tiers (self-contained; VM cannot import workbay packages).
    # Align with orchestrator is_shaped_result_payload: known handoff_action
    # value OR list-valued findings — key presence alone is not shape.
    # DURREV-VM-F2: do not claim byte-parity with host extract; rank by
    # authority (structuredOutput/structured_output before narrated channels).
    _KNOWN_HANDOFF_ACTIONS = frozenset({"merge_ready", "needs_guidance"})

    def _is_shaped(d):
        if not isinstance(d, dict):
            return False
        action = d.get("handoff_action")
        if isinstance(action, str) and action in _KNOWN_HANDOFF_ACTIONS:
            return True
        return isinstance(d.get("findings"), list)

    def _iter_balanced_objects(text):
        """Yield brace-balanced {...} slices; string context + escapes respected.

        _BACKSLASH is spelled chr(92) on purpose. This body rides the unquoted
        remote heredoc, and bash collapses a literal backslash-backslash to a
        single backslash while constructing the remote script. Written the
        obvious way, the comparison arrives on the VM as an unterminated string
        literal and every post-processing run dies with SyntaxError, stranding
        the agent's committed work behind a zero-byte turn.patch [AGT-10].

        (No heredoc-delimiter token spelled out in this comment: the region
        scanner treats any such token as an opener, same trap the backtick
        warnings above guard against.)
        """
        _BACKSLASH = chr(92)
        i = 0
        n = len(text)
        while i < n:
            start = text.find("{", i)
            if start == -1:
                return
            depth = 0
            in_str = False
            escaped = False
            end = None
            for j in range(start, n):
                ch = text[j]
                if in_str:
                    if escaped:
                        escaped = False
                    elif ch == _BACKSLASH:
                        escaped = True
                    elif ch == '"':
                        in_str = False
                    continue
                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = j
                        break
            if end is None:
                # Unbalanced "{" (truncated or narrated code fragment). Do NOT
                # abandon the scan: a complete shaped object may follow it.
                # Skip this brace and re-scan from the next one (REVC-002).
                i = start + 1
                continue
            block = text[start : end + 1]
            yield block
            # False-balance engulf (DURREV-VM-F4): a non-JSON prefix brace can
            # close around a later real object. When the slice is not valid
            # JSON, resume one past the opener so the inner object is seen.
            try:
                json.loads(block)
            except json.JSONDecodeError:
                i = start + 1
                continue
            i = end + 1

    def _text_last_shaped(text):
        """Return the last shaped object in text, or None (REVC-004 / F1)."""
        last = None
        for block in _iter_balanced_objects(text):
            try:
                obj = json.loads(block)
            except json.JSONDecodeError:
                continue
            if _is_shaped(obj):
                last = obj
        return last

    def _structured_shaped(value):
        if isinstance(value, dict) and _is_shaped(value):
            return value
        if isinstance(value, str):
            # Prefer whole-string parse; else last shaped balanced object.
            try:
                so = json.loads(value)
            except json.JSONDecodeError:
                so = None
            if _is_shaped(so):
                return so
            return _text_last_shaped(value)
        return None

    # Authority-ranked recovery (honest; not byte-parity with host extract):
    #   1) root shape
    #   2) structuredOutput or structured_output (beats narrated channels — REVC-003)
    #   3) dict-valued envelope fields (HARM-03; host _extract_review_payload)
    #   4) string channels, first channel with a shaped object, last-wins inside
    # Recovered inner payloads are PROMOTED onto the artifact (DURREV-VM-F1).
    recovered = None
    if isinstance(payload, dict):
        if _is_shaped(payload):
            recovered = payload
        else:
            # Preserve camelCase precedence when both spellings are present.
            so_key = "structuredOutput" if "structuredOutput" in payload else "structured_output"
            so_hit = _structured_shaped(payload.get(so_key))
            if so_hit is not None:
                recovered = so_hit
            if recovered is None:
                for key in ("result", "content", "output", "message"):
                    value = payload.get(key)
                    if isinstance(value, dict) and _is_shaped(value):
                        recovered = value
                        break
            if recovered is None:
                for key in ("text", "output_text", "content", "message", "result"):
                    value = payload.get(key)
                    if isinstance(value, str):
                        cand = _text_last_shaped(value)
                        if cand is not None:
                            recovered = cand
                            break
    return payload, loaded, recovered

def _successful_result_artifact():
    if agent_rc != 0:
        return False
    import os

    path = (
        os.environ.get("AGENT_SPEC_RESULT_FILE") or ""
        if spec.get("result_source") == "output_last_message"
        else stdout_path
    )
    if not path:
        return False
    try:
        raw = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    payload, _, recovered = _recover_result(raw)
    return recovered is not None or (
        isinstance(payload, dict) and any(k in payload for k in _RESULTISH_KEYS)
    )

if patterns and any(p in blob for p in patterns):
    if not _successful_result_artifact():
        print("auth_failed")
        raise SystemExit(0)
    print(
        "remote_agent: auth pattern ignored: rc=0 with result-shaped artifact (echo suspected)",
        file=sys.stderr,
    )
if service_tier_warning_patterns and any(p in service_tier_warning_blob for p in service_tier_warning_patterns):
    matching_line = next(
        line
        for line in service_tier_warning_blob.splitlines()
        if any(pattern in line for pattern in service_tier_warning_patterns)
    )
    print(f"remote_agent: requested service tier unconfirmed: {matching_line[:200]}", file=sys.stderr)
    print("service_tier_unconfirmed")
    raise SystemExit(0)
if agent_rc != 0:
    print("agent_failed")
    raise SystemExit(0)

# D9: non-empty *result artifact* that is not review/edit-shaped → degraded.
# result_source=stdout → parse the stdout redirect (schema JSON).
# result_source=output_last_message → parse AGENT_SPEC_RESULT_FILE (-o),
# NEVER the JSONL stream on stdout (that would false-degrade every turn).
import os

result_source = str(spec.get("result_source") or "stdout")
if result_source == "output_last_message":
    result_path = os.environ.get("AGENT_SPEC_RESULT_FILE") or ""
else:
    result_path = stdout_path
if not result_path:
    print("ok")
    raise SystemExit(0)
try:
    raw = Path(result_path).read_text(encoding="utf-8", errors="replace")
except OSError as exc:
    print(f"remote_agent: result artifact unreadable: {exc}", file=sys.stderr)
    print("result_degraded")
    raise SystemExit(0)
if raw.strip():
    def _write_degraded(src: str) -> None:
        # A read-only or full artifact path must not surface as a Python
        # traceback with a zero-length status: the caller reads stdout as the
        # outcome token, so an unwritable rewrite gets its own typed token.
        try:
            Path(result_path).write_text(
                json.dumps(
                    {
                        "findings": [],
                        "summary": "result_unparseable",
                        "result_parse": "degraded",
                        "raw_tail": src[:800],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            print(
                f"remote_agent: result rewrite failed for {result_path}: {exc}",
                file=sys.stderr,
            )
            print("result_rewrite_failed")
            raise SystemExit(0)

    payload, loaded, recovered = _recover_result(raw)
    if payload is None:
        _write_degraded(raw)
        print("result_degraded")
        raise SystemExit(0)

    shaped = recovered is not None
    if not shaped:
        # Tier two (unshaped): well-formed dict that failed the shape gate but
        # still carries worker/result keys. Keep the payload so a committed
        # turn's summary/tests_run survive; the orchestrator clamps
        # handoff_action fail-closed. Do NOT collapse this into unparseable.
        # Source of truth: _result_text.py _RESULTISH_KEYS (VM cannot import).
        if not (
            isinstance(payload, dict)
            and any(k in payload for k in _RESULTISH_KEYS)
        ):
            # Tier three: non-dict, non-JSON, or CLI session/usage envelope.
            _write_degraded(raw)
            print("result_degraded")
            raise SystemExit(0)
    else:
        # Promote recovered nested/stream payload onto the artifact so the
        # consumer does not re-derive it (DURREV-VM-F1 / HG0804-27-B).
        if recovered is not payload or loaded is None:
            try:
                Path(result_path).write_text(
                    json.dumps(recovered) + "\n",
                    encoding="utf-8",
                )
                payload = recovered
            except OSError as exc:
                # OBS-08 / AGT-10 / REF-37 / RLSE-05: rewrite failure must not
                # look like success — announce path + error and emit a distinct
                # post token.
                print(
                    f"remote_agent: result rewrite failed for {result_path}: {exc}",
                    file=sys.stderr,
                )
                print("result_rewrite_failed")
                raise SystemExit(0)
print("ok")
PY
)
    case "\$_post" in
        rate_limited)
            _salvage_committed_work || true
            _terminate_unsafe_harvest 9
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: rate_limited (upstream quota): exit 9' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            exit 9
            ;;
        auth_failed)
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: auth_match failure (exit 6)' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            exit 6
            ;;
        service_tier_unconfirmed)
            _salvage_committed_work || true
            _terminate_unsafe_harvest 10
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: requested service tier unconfirmed (exit 10)' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            exit 10
            ;;
        result_degraded)
            _salvage_committed_work || true
            _terminate_unsafe_harvest 5
            echo 'remote_agent: result present but unparseable (exit 5)' >&2
            exit 5
            ;;
        result_rewrite_failed)
            _salvage_committed_work || true
            _terminate_unsafe_harvest 5
            echo 'remote_agent: result rewrite failed after multi-object salvage (exit 5)' >&2
            exit 5
            ;;
        agent_failed)
            _salvage_committed_work || true
            _agent_failed_exit="\$(_classify_agent_failed_exit)"
            _terminate_unsafe_harvest "\$_agent_failed_exit"
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: agent run failed:' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            # Wall-clock expiry is its own transport status (exit 8);
            # _salvage_committed_work (git format-patch when BASE..HEAD diverges)
            # still runs so partial commits return from an expired lane.
            exit "\$_agent_failed_exit"
            ;;
        ok) ;;
        *)
            _salvage_committed_work || true
            _terminate_unsafe_harvest 3
            echo "remote_agent: post-classify unknown status: \${_post}" >&2
            exit 3
            ;;
    esac
    _sanitize_harvest_gitdir || true
    _terminate_unsafe_harvest 3
    if ! _harvest_git merge-base --is-ancestor "\$BASE" HEAD >/dev/null 2>&1; then
        _SALVAGE_DONE=1
        echo 'remote_agent: source_identity_failure=source_history_not_descendant refusing success harvest outside sandbox base lineage' >&2
        exit 17
    fi
    if _harvest_git diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD; then
        echo 'remote_agent: agent produced no committed changes' >&2
        # >>> exit-4-uncommitted-salvage
        # (has_commits=no, has_dirty=?): BASE..HEAD is quiet, so committed
        # format-patch cannot recover anything and the working tree is the
        # only place work can be. This cell owns stdout outright -- there is
        # no mbox stream to protect -- so the capture goes to the host --out
        # directly, BEFORE sandbox teardown. Keep exit 4 and _SALVAGE_DONE.
        # The capture recipe itself lives in _capture_uncommitted_work so the
        # committed arms can reuse it rather than re-derive it [GRPH-27].
        _e4_tmp=\$(mktemp "\${TMPDIR:-/tmp}/ra-e4-uncommitted.XXXXXX") || true
        if [ -n "\${_e4_tmp:-}" ]; then
            if _capture_uncommitted_work "\$_e4_tmp"; then
                cat "\$_e4_tmp"
            fi
            rm -f "\$_e4_tmp"
        fi
        # <<< exit-4-uncommitted-salvage
        _SALVAGE_DONE=1
        exit 4
    fi
    _emit_off_box_selfverify
    # Success-path empty-patch guard (DURFIX-SALVAGE-EMPTY-PATCH-01): HEAD !=
    # BASE was already established; a 0-byte format-patch must not look like
    # success. Exit 3 (hard failure class) — not a new exit-1 producer phrase
    # (adapter exit-1 map is owned by a sibling lane).
    _ok_patch_tmp=\$(mktemp "\${TMPDIR:-/tmp}/ra-ok-patch.XXXXXX") || {
        echo 'remote_agent: mktemp failed before format-patch (exit 3)' >&2
        exit 3
    }
    if ! _harvest_git format-patch --no-textconv "\$BASE"..HEAD --stdout > "\$_ok_patch_tmp"; then
        echo 'remote_agent: format-patch failed while HEAD != BASE (exit 3)' >&2
        rm -f "\$_ok_patch_tmp"
        exit 3
    fi
    if [ ! -s "\$_ok_patch_tmp" ]; then
        echo 'remote_agent: format-patch empty while HEAD != BASE (exit 3)' >&2
        echo "remote_agent: BASE=\$BASE HEAD=\$(_harvest_git rev-parse HEAD) branch=\$(_harvest_git rev-parse --abbrev-ref HEAD)" >&2
        rm -f "\$_ok_patch_tmp"
        exit 3
    fi
    _SALVAGE_DONE=1
    # (has_commits=yes, has_dirty=yes) on the SUCCESS path. This is the cell
    # that used to lose data most quietly: the patch is well-formed and the
    # exit code is 0, so nothing downstream had any reason to look [GRPH-27].
    _emit_uncommitted_remainder
    cat "\$_ok_patch_tmp"
    rm -f "\$_ok_patch_tmp"
    exit 0
fi
# <<< agent-exec
echo 'remote_agent: --agent-spec required (legacy no-spec path deleted)' >&2
exit 2
REMOTE_EOF
    }

    run_patch() {
        # implementation note: capture resolver source once so the unquoted heredoc can
        # inject it via an allowlisted host expansion (not a bare $(...) site).
        _AGENT_SPEC_RESOLVER_SRC="$(declare -f _agent_spec_resolve_argv)"
        _review_model_may_have_started=1
        # implementation note S1 / REV0192R12-B-1: ssh is a grandchild of the adapter.
        # Only this script can honestly stamp call/return around the heredoc.
        _emit_host_instant ssh_call_ts "$(date +%s)"
        # Capture rc without set -e abort so ssh_return_ts still stamps [fail-open].
        _run_patch_rc=0
        # streamed via bash -s <<REMOTE_EOF
        # shellcheck disable=SC2029
        _emit_remote_body | "${SSH[@]}" env "WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB=$UV_CACHE_CAP_MB" bash -s || _run_patch_rc=$?
        # Immediately after heredoc returns — before any artifact fetch [REV0192R12-B-1].
        _emit_host_instant ssh_return_ts "$(date +%s)"
        return "$_run_patch_rc"
    }

    run_submit() {
        _review_model_may_have_started=1
        _AGENT_SPEC_RESOLVER_SRC="$(declare -f _agent_spec_resolve_argv)"
        _SUBMIT_JOB_SRC="$(declare -f _remote_submit_job)"
        _body_b64="$(_emit_remote_body | base64 | tr -d '\n')"
        _admission_b64="$(printf '%s' "$_admission_remote_sh" | base64 | tr -d '\n')"
        if [ "$GROK_TIMEOUT" -gt 0 ]; then
            JOB_RUNTIME_SEC=$(( GROK_TIMEOUT + 120 ))
        else
            JOB_RUNTIME_SEC=$(( WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S + 120 ))
        fi
        _emit_host_instant submit_call_ts "$(date +%s)"
        _run_submit_rc=0
        # shellcheck disable=SC2029
        "${SSH[@]}" env \
            "WORKBAY_REMOTE_AGENT_UV_CACHE_CAP_MB=$UV_CACHE_CAP_MB" \
            "WORKBAY_REMOTE_SUBMIT_CRASH_AFTER=${WORKBAY_REMOTE_SUBMIT_CRASH_AFTER:-}" \
            bash -s <<SUBMIT_EOF || _run_submit_rc=$?
set -euo pipefail
${_SUBMIT_JOB_SRC}
export JOB_ID='${JOB_ID}'
export LANE_KEY='${LANE_KEY}'
export AGENT_ROOT='${AGENT_ROOT}'
export MEM_MAX='${MEM_MAX}'
export CPU_QUOTA='${CPU_QUOTA}'
export JOB_RUNTIME_SEC='${JOB_RUNTIME_SEC}'
export UV_CACHE_CAP_MB='${UV_CACHE_CAP_MB}'
export DISPATCH_NONCE='${DISPATCH_NONCE}'
export QUEUE_TICK_INTERVAL_S='${QUEUE_TICK_INTERVAL_S}'
BODY_B64='${_body_b64}'
ADMISSION_B64='${_admission_b64}'
case "\$AGENT_ROOT" in /*) ROOT="\$AGENT_ROOT" ;; *) ROOT="\$HOME/\$AGENT_ROOT" ;; esac
export BRIEF_PATH="\$ROOT/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md"
export OUTBOX_PATH="\$ROOT/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}"
export REF_NAME='refs/heads/${LANE_KEY}-${DISPATCH_NONCE}'
export REMOTE_GIT="\$HOME/${REMOTE_DIR}"
export WORKBAY_REMOTE_SUBMIT_CRASH_AFTER="\${WORKBAY_REMOTE_SUBMIT_CRASH_AFTER:-}"
_remote_submit_job
SUBMIT_EOF
        _emit_host_instant submit_return_ts "$(date +%s)"
        if [ "$_run_submit_rc" -eq 0 ]; then
            _dispatch_staged=0
        fi
        return "$_run_submit_rc"
    }

    _fetch_outbox_artifact() {
        local _fo_dest="$1"
        local _fo_nonempty="$2"
        local _fo_name
        local _fo_tmp="${_fo_dest}.fetch.$$"
        local _fo_dir="${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}"
        for _fo_name in "${@:3}"; do
            if _scp_with_deadline "${_fo_dir}/${_fo_name}" "$_fo_tmp" 2>/dev/null; then
                if [ -f "$_fo_tmp" ] \
                    && { [ "$_fo_nonempty" != 1 ] || [ -s "$_fo_tmp" ]; } \
                    && mv -f "$_fo_tmp" "$_fo_dest"; then
                    return 0
                fi
            fi
            rm -f "$_fo_tmp" 2>/dev/null || true
        done
        return 1
    }

    fetch_result() {
        [ -n "$RESULT_OUT" ] || return 0
        # Best-effort: the agent's stdout JSON lives in this dispatch's outbox (outside
        # $SBX), so fetch it even on a no-change / agent-fail exit — the caller can
        # still surface the summary/blockers. Missing file is non-fatal.
        if _fetch_outbox_artifact "$RESULT_OUT" 0 .agent-result.json .grok-result.json; then
            echo "remote_agent: result written -> $RESULT_OUT" >&2
        else
            echo "remote_agent: no result json fetched (agent emitted no stdout?)" >&2
        fi
    }

    fetch_debug() {
        [ -n "$DEBUG_OUT" ] || return 0
        # Best-effort, mirroring fetch_result: the caller runs the post-turn
        # grok-build contamination backstop on this log. Missing file is non-fatal
        # (an absent/empty log is "no contamination", same as GrokCliAdapter).
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-debug.log" "$DEBUG_OUT" 2>/dev/null; then
            echo "remote_agent: debug log written -> $DEBUG_OUT" >&2
        else
            echo "remote_agent: no debug log fetched (grok emitted no --debug-file?)" >&2
        fi
    }

    fetch_stream() {
        [ -n "$STREAM_OUT" ] || return 0
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.agent-stream.jsonl" "$STREAM_OUT" 2>/dev/null; then
            echo "remote_agent: agent stream written -> $STREAM_OUT" >&2
        else
            echo "remote_agent: no agent stream fetched" >&2
        fi
    }

    fetch_selfverify() {
        [ -n "$SELFVERIFY_OUT" ] || return 0
        # Best-effort, mirroring fetch_result: the off-box self-verify JSON lives in
        # this dispatch's outbox. Missing file is non-fatal — the worker's OBS-08
        # enforcement blocks a commit-landed lane with no capture (never a silent pass).
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-selfverify.json" "$SELFVERIFY_OUT" 2>/dev/null; then
            echo "remote_agent: self-verify result written -> $SELFVERIFY_OUT" >&2
        else
            echo "remote_agent: no self-verify result fetched (off-box verify not run / no commit?)" >&2
        fi
    }

    fetch_uncommitted() {
        [ -n "$UNCOMMITTED_OUT" ] || return 0
        # The remainder is a separate, nonce-scoped artifact: never concatenate
        # it with the committed mbox stream on --out. Stage then rename so a
        # failed scp cannot leave a stale prior remainder looking current
        # [DATA-13][OBS-08].
        _uncommitted_fetch_tmp="${UNCOMMITTED_OUT}.fetch.$$"
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/uncommitted-remainder.patch" \
                "$_uncommitted_fetch_tmp" 2>/dev/null \
            && [ -s "$_uncommitted_fetch_tmp" ] \
            && mv -f "$_uncommitted_fetch_tmp" "$UNCOMMITTED_OUT"; then
            echo "remote_agent: uncommitted remainder written -> $UNCOMMITTED_OUT" >&2
            return 0
        else
            rm -f "$_uncommitted_fetch_tmp" "$UNCOMMITTED_OUT" 2>/dev/null || true
            echo "remote_agent: no uncommitted remainder fetched" >&2
            return 0
        fi
    }

    # implementation note S1: phases fetch is script-owned. Remote path is nonce-scoped
    # [CON-12]; $OUT_DIR is VM-only — never reference it on the local side.
    fetch_phases() {
        [ -n "$PHASES_OUT" ] || return 0
        # Stage into a temp path then rename so a failed scp cannot leave a
        # prior non-empty PHASES_OUT looking like this dispatch's ok [A-04].
        if _fetch_outbox_artifact "$PHASES_OUT" 1 .agent-phases.json .grok-phases.json; then
            echo "remote_agent: phases written -> $PHASES_OUT" >&2
            return 0
        else
            # Drop a stale prior file so status cannot claim ok after fetch_rc=1.
            rm -f "$PHASES_OUT" 2>/dev/null || true
            echo "remote_agent: no phases json fetched (pre-materialize miss or scp failure)" >&2
            return 1
        fi
    }

    fetch_provenance() {
        [ -n "$PROVENANCE_OUT" ] || return 0
        _provenance_fetch_tmp="${PROVENANCE_OUT}.fetch.$$"
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.workbay-source-provenance.json" \
                "$_provenance_fetch_tmp" 2>/dev/null \
            && [ -s "$_provenance_fetch_tmp" ] \
            && mv -f "$_provenance_fetch_tmp" "$PROVENANCE_OUT"; then
            echo "remote_agent: provenance receipt written -> $PROVENANCE_OUT" >&2
        else
            rm -f "$_provenance_fetch_tmp" "$PROVENANCE_OUT" 2>/dev/null || true
            echo "remote_agent: no provenance receipt fetched" >&2
        fi
    }

    rc=0
    _review_model_may_have_started=1
    if [ "$cmd" = submit ]; then
        _phase "submitting detached remote grok job ${JOB_ID} (residual ${GROK_TIMEOUT}s)"
        if run_submit; then rc=0; else rc=$?; fi
        if [ "$rc" -eq 0 ]; then
            _review_cycle_finished=1
            _review_cycle_bridge pending "$JOB_ID" "$REVIEW_SUBJECT_BRANCH" "$REVIEW_KIND" "$AGENT_SPEC" "$CANON_PROPOSAL_PATH" "$rc" "" "" "" "$DISPATCH_NONCE" "refs/heads/${LANE_KEY}-${DISPATCH_NONCE}" || true
        fi
        exit "$rc"
    fi
    _phase "materializing sandbox + dispatching remote grok build (residual ${GROK_TIMEOUT}s)"
    if [ "$REVIEW_KIND" = analysis ]; then
        # Analysis output is captured, never streamed: a mutation must not
        # reach the caller's stdout or --out before the post-run check below.
        _review_patch_evidence="$_review_spool/patch"
        if run_patch > "$_review_patch_evidence"; then rc=0; else rc=$?; fi
    elif [ -n "$OUT" ]; then
        if run_patch > "$OUT"; then rc=0; else rc=$?; fi
        if [ "$rc" -eq 0 ]; then
            echo "remote_agent: patch written -> $OUT ($(grep -c '^Subject:' "$OUT") commit(s))" >&2
        fi
    else
        if run_patch | tee "$_review_spool/patch"; then rc=0; else rc=$?; fi
    fi
    # Three-way fetch gate (S1.4): fetch_phases is unconditional w.r.t. rc so a
    # post-materialize exit-75 partial still lands; result/debug/selfverify stay
    # behind the existing 75/78 gate (lock-loser must not pull another's outbox).
    _phases_fetch_rc=0
    if fetch_phases; then _phases_fetch_rc=0; else _phases_fetch_rc=$?; fi
    if [ "$rc" -ne 75 ] && [ "$rc" -ne 78 ]; then
        fetch_result
        fetch_debug
        fetch_stream
        fetch_selfverify
        fetch_uncommitted
        fetch_provenance
    fi
    # A read-only analysis is mediated again after the run (SECD-03): any
    # returned patch (commits, or the uncommitted tree diff of an exit-4 run),
    # a moved HEAD, or provenance naming another source commit/tree refuses it.
    # The patch stays in the spool as settlement evidence only; the attempt
    # stays charged and never reaches the caller.
    if [ "$REVIEW_KIND" = analysis ]; then
        _analysis_refusal=""
        if [ -s "$_review_patch_evidence" ]; then
            _analysis_refusal="analysis_mutated_source"
        elif [ "$(git rev-parse HEAD 2>/dev/null)" != "$ANALYSIS_SOURCE_HEAD" ]; then
            _analysis_refusal="analysis_source_head_moved"
        elif [ -s "$PROVENANCE_OUT" ] && ! python3 - "$PROVENANCE_OUT" "$ANALYSIS_SOURCE_HEAD" "$ANALYSIS_SOURCE_TREE" <<'PY'
import json, sys

try:
    proof = json.load(open(sys.argv[1], encoding="utf-8"))
    same = (proof["remote_source_commit"], proof["remote_source_tree"]) == (sys.argv[2], sys.argv[3])
except (OSError, ValueError, KeyError, TypeError):
    same = False
sys.exit(0 if same else 1)
PY
        then
            _analysis_refusal="analysis_source_tree_mismatch"
        fi
        if [ -n "$_analysis_refusal" ]; then
            mv -f "$_review_patch_evidence" "$_review_spool/refused-analysis.patch"
            _review_patch_evidence="$_review_spool/refused-analysis.patch"
            [ -z "$OUT" ] || rm -f "$OUT"
            echo "remote_agent: review_cycle_analysis refused: ${_analysis_refusal}" >&2
            rc=76
        elif [ -n "$OUT" ]; then
            # Clean analysis: the requested --out is the empty capture.
            mv -f "$_review_patch_evidence" "$OUT"
            _review_patch_evidence="$OUT"
        fi
    fi
    # One compact summary ≤200 chars; last body write before exit (EXIT trap may
    # follow — that shape is green, not a defect) [REV0192R3-A6].
    # Status is AND of non-empty local file AND successful fetch this dispatch
    # [REV0192S1-A-04]: a stale prior PHASES_OUT must not print phases=ok with
    # fetch_rc=1.
    _phases_status="miss"
    if [ -z "${PHASES_OUT:-}" ]; then
        _phases_status="no-flag"
    elif [ "${_phases_fetch_rc:-1}" -eq 0 ] && [ -s "${PHASES_OUT}" ]; then
        _phases_status="ok"
    else
        _phases_status="miss"
    fi
    echo "remote_agent: phases summary rc=${rc} phases=${_phases_status} fetch_rc=${_phases_fetch_rc}" >&2
    _review_cycle_finished=1
    if [ -z "${WORKBAY_REVIEW_CYCLE_TOKEN:-}" ]; then
        _review_cycle_bridge settle "$JOB_ID" "$REVIEW_SUBJECT_BRANCH" "$REVIEW_KIND" "$AGENT_SPEC" "$CANON_PROPOSAL_PATH" "$rc" "$RESULT_OUT" "$PROVENANCE_OUT" "${_review_patch_evidence:-${OUT:-$_review_spool/patch}}" || true
    fi
    exit "$rc"
    # END BUILD_DISPATCH
    ;;
*)
    sed -n '15,26p' "$0" >&2
    exit 2
    ;;
esac
