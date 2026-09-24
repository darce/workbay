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
#   scripts/remote_agent.sh reap [--idle-seconds N] [--dry-run]
#   scripts/remote_agent.sh status [--sweep] --job-id <j+20hex> [--job-id ...]
#   scripts/remote_agent.sh collect --job-id <j+20hex>
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
#   --phases-out FILE  fetch the per-dispatch phase-timing record (.grok-phases.json) to
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
#   WORKBAY_REMOTE_AGENT_ROOT  sandbox parent dir (default grok-sandbox).
#   WORKBAY_REMOTE_GATE_MEMORY_MAX / _CPU_QUOTA   per-run caps (6G / 200%).
#   WORKBAY_REMOTE_GATE_MEM_FLOOR_MB   VM MemAvailable floor (default 2048); the lane
#                      defers (exit 75) below this so non-lane work keeps its headroom.
#   WORKBAY_REMOTE_AGENT_MAX_LANES     concurrent grok-lane-* scopes on the VM
#                      (default 20, must be an integer >= 1); at/above the cap the
#                      lane defers (exit 75).
#   WORKBAY_REMOTE_GATE_PID_FLOOR      free pids the user slice must retain
#                      (default 64); systemd charges threads to pids.max and one
#                      lane costs ~25-30, so memory/lane-count both read healthy
#                      at pid saturation. 0 disables the dimension.
#   WORKBAY_REMOTE_GATE_PIDS_ROOT      cgroup dir holding pids.current/pids.max
#                      (default /sys/fs/cgroup/user.slice/user-$(id -u).slice).
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

repo_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
repo_slug="$(basename "$repo_root")"

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
_env_agent_root="${WORKBAY_REMOTE_AGENT_ROOT:-}"
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
AGENT_ROOT="${_env_agent_root:-grok-sandbox}"
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
PREFLIGHT_PROBE_NONCE="${_env_probe_nonce:-workbay-probe-$$}"
# VM MemAvailable floor (MiB): defer the lane when the VM is below this, reserving
# headroom for ALL non-lane work on the box (co-resident mission-critical procs).
MEM_FLOOR_MB="${_env_mem_floor:-2048}"
# Concurrent named grok-lane-* scopes on the VM (implementation note S5; [RES-14] backpressure).
# Default 20 matches the measured VM envelope: a 19-lane stress run held a 17.2GB
# MemAvailable floor, so RAM is not the binding constraint; CPU during sandbox
# materialization is. MEM_FLOOR_MB remains the real backpressure for co-resident
# mission-critical procs. Must be >= 1 (MAX_LANES=0 would permanently defer every
# turn with no useful signal).
MAX_LANES="${_env_max_lanes:-20}"
# Free pids the user slice must retain for a lane to be admitted
# (LANDCAMP-VM-H-02). MAX_LANES alone cannot see this: systemd counts *threads*
# against pids.max, a lane costs ~25-30 of them, and the observed slice cap was
# 512 — so ~20 lanes exhausts the pid budget while MemAvailable and the
# grok-lane-* scope count both still read healthy. Past exhaustion fork() returns
# EAGAIN and lane edits are silently lost. Default 64 reserves room for one more
# lane plus slack. A probe that cannot read the cgroup fails OPEN, matching the
# other two dimensions — deferring on a read error would turn a probe glitch into
# a total dispatch outage. [RES-14] [CON-04] [DIAG-07]
PID_FLOOR="${WORKBAY_REMOTE_GATE_PID_FLOOR:-64}"
# Retention cap for PERSISTED per-lane venvs (internal S4):
# venvs survive the per-pass wipe by design, so they accumulate one per distinct
# offloaded branch and would grow the VM disk unbounded. Keep the N most-recently-
# used; LRU-evict the rest (with their sync stamps) at build time. 0 = keep all
# (disable reap). Default 24 > MAX_LANES so warm reuse survives normal rotation --
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
case "$REMOTE_DIR" in ""|.|/*|*..*|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid REMOTE_DIR" >&2; exit 2 ;; esac
case "$AGENT_ROOT" in ""|/*|*..*|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid AGENT_ROOT" >&2; exit 2 ;; esac
case "$MEM_MAX" in *[!0-9GMK]*|"") echo "remote_agent: MEMORY_MAX must look like 6G/512M" >&2; exit 2 ;; esac
case "$CPU_QUOTA" in *[!0-9%]*|"") echo "remote_agent: CPU_QUOTA must look like 200%" >&2; exit 2 ;; esac
case "$MEM_FLOOR_MB" in *[!0-9]*|"") echo "remote_agent: MEM_FLOOR_MB must be an integer (MiB)" >&2; exit 2 ;; esac
case "$MAX_LANES" in *[!0-9]*|"") echo "remote_agent: MAX_LANES must be an integer >= 1" >&2; exit 2 ;; esac
case "$PID_FLOOR" in *[!0-9]*|"") echo "remote_agent: PID_FLOOR must be a non-negative integer (0=disable the pid dimension)" >&2; exit 2 ;; esac
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
case "$PREFLIGHT_PROBE_NONCE" in ""|*[!A-Za-z0-9._:-]*) echo "remote_agent: WORKBAY_PROBE_NONCE contains unsupported characters" >&2; exit 2 ;; esac
case "$KEEP_REFS" in *[!A-Za-z0-9/_.\ -]*) echo "remote_agent: KEEP_REFS may only contain letters, digits, / _ . - and spaces" >&2; exit 2 ;; esac
case "$REAP_LEGACY_REFS" in 0|1) ;; *) echo "remote_agent: REAP_LEGACY_REFS must be 0 or 1" >&2; exit 2 ;; esac
if [ "$MAX_LANES" -lt 1 ]; then
    echo "remote_agent: MAX_LANES must be an integer >= 1" >&2
    exit 2
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
    ROOT="$HOME/${AGENT_ROOT}"
    JOB_DIR="$ROOT/.jobs/${JOB_ID}"
    UNIT="grok-lane-${LANE_KEY}-job-${JOB_ID}.service"

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
        [ -n "${_outbox}" ] && rm -rf "${_outbox}"
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
    _submit_start_unit() {
        _submit_require_linger
        timeout 30 systemd-run --quiet --user --unit "$UNIT" --service-type=exec \
            -p "RuntimeMaxSec=${JOB_RUNTIME_SEC}" \
            -p "MemoryMax=${MEM_MAX}" \
            -p "CPUQuota=${CPU_QUOTA}" \
            --working-directory="$JOB_DIR" \
            /bin/bash "$JOB_DIR/wrapper.sh"
    }

    # Linger before the first job on this connection. Retries that only observe
    # (done / active / lost) must not fail if linger is later disabled.
    # Take the per-job lock before the new-versus-existing decision so two
    # concurrent first submits cannot both claim (CON-11).
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

    _submit_require_linger
    if [ -z "$_state" ]; then
        _submit_write_state claiming
        _submit_crash_if claiming
    fi
    _submit_write_body_and_env
    _submit_write_state ready
    _submit_crash_if ready
    _submit_start_unit
    _submit_crash_if systemd-run
    _submit_write_state submitted
    _submit_print "submitted"
    exit 0
}
# END REMOTE_SUBMIT_JOB

# BEGIN REMOTE_JOB_OPS
# Host-testable VM helpers for status (batched), collect, cancel, and sweep.
# Nested helpers are defined at call time so `declare -f _remote_job_ops` is
# the single payload shipped over one ssh round trip (RES-12).
_remote_job_ops() {
    set -euo pipefail
    : "${AGENT_ROOT:?}"
    ROOT="$HOME/${AGENT_ROOT}"

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

    _remote_job_sweep() {
        [ -d "$ROOT/.jobs" ] || return 0
        for _jd in "$ROOT/.jobs"/*; do
            [ -d "$_jd" ] || continue
            _jid="$(basename "$_jd")"
            _collected=""
            if [ -f "$_jd/state.json" ]; then
                _collected="$(_remote_job_json_field "$_jd/state.json" collected_at)"
            fi
            if [ -n "$_collected" ]; then
                _age="$(_remote_job_iso_age_s "$_collected" || true)"
                if [ -n "${_age:-}" ] && [ "$_age" -gt 86400 ]; then
                    rm -rf "$_jd"
                    printf 'removed %s age_s=%s\n' "$_jid" "$_age"
                    continue
                fi
            fi
            _remote_job_compute_state "$_jd"
            if [ "$_out_state" = "done" ] && [ -z "$_collected" ]; then
                _age="$(_remote_job_file_age_s "$_jd/done.json" || _remote_job_file_age_s "$_jd" || echo 0)"
                if [ "$_age" -gt 604800 ]; then
                    printf 'uncollected_stale %s age_s=%s\n' "$_jid" "$_age"
                fi
                continue
            fi
            if [ "$_out_state" = "lost" ]; then
                _age="$(_remote_job_file_age_s "$_jd" || echo 0)"
                if [ "$_age" -gt 604800 ]; then
                    rm -rf "$_jd"
                    printf 'removed %s age_s=%s\n' "$_jid" "$_age"
                fi
            fi
        done
    }

    _remote_job_status() {
        QUEUE_STALE="$(_remote_queue_stale)"
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
            mkdir -p "$JOB_DIR"
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
        tar -cf - $_list
    }

    _remote_job_cancel() {
        : "${JOB_ID:?}"
        JOB_DIR="$ROOT/.jobs/${JOB_ID}"
        if [ ! -d "$JOB_DIR" ]; then
            echo "remote_agent: job_not_found" >&2
            exit 2
        fi
        _unit=""
        if [ -f "$JOB_DIR/state.json" ]; then
            _unit="$(_remote_job_json_field "$JOB_DIR/state.json" unit)"
        fi
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
        mkdir -p "$JOB_DIR"
        exec 9>"$JOB_DIR/.lock"
        flock -w 30 9 || { echo "remote_agent: cancel_lock_timeout" >&2; exit 75; }
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
            fi
            printf '{"finished_at":"%s","job_id":"%s","rc":8,"reason":"cancelled","schema_version":1,"stdout_bytes":%s,"stdout_sha256":"%s"}\n' \
                "$_now" "$JOB_ID" "${_bytes:-0}" "$_sha" > "$JOB_DIR/done.json.tmp"
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
args = sys.argv[1:10]
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
                             "bash", "-s", "--", *args], stdin=subprocess.PIPE,
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
ssh_argv = list(sys.argv[10:])
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
    python3 -c "$_preflight_driver" "$_probe_kind" "$_probe_timeout" "$_probe_model" "$_probe_cap" "$AGENT_ROOT" "$_probe_nonce" "${WORKBAY_PREFLIGHT_PROBE_EFFORT:-low}" "${WORKBAY_PREFLIGHT_PROBE_BINARY:-.local/bin/codex}" "${WORKBAY_PREFLIGHT_PROBE_PATH:-.local/bin}" "${SSH[@]}" <<'REMOTE_PREFLIGHT_BODY_EOF'
set -u
kind="${1:-}"
bound="${2:-}"
model="${3:-}"
cap="${4:-2}"
agent_root="${5:-grok-sandbox}"
nonce="${6:-workbay-probe-${BASHPID:-$$}}"
effort="${7:-low}"
export WORKBAY_PROBE_NONCE="$nonce"
started="$(date +%s)"
slot_root="$HOME/$agent_root/.workbay-preflight-probes"
mkdir -p -- "$slot_root" 2>/dev/null || true
emit() {
    _ok="$1"; _verdict="$2"; _reason="$3"; _head="${4:-}"; _now="$(date +%s)"
    _elapsed=$((_now - started))
    printf '{"schema":"workbay.preflight.v1","probe":"%s","ok":%s,"verdict":"%s","reason":"%s","elapsed_s":%s,"live_probe_count":%s,"head":"%s","model":"%s","nonce":"%s","sandbox_flags":"%s"}\n' \
        "$kind" "$_ok" "$_verdict" "$_reason" "$_elapsed" "${live_count:-0}" "$_head" "$model" "$nonce" "${sandbox_flags:-workspace-write}"
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
    if [ -n "${probe_root:-}" ]; then rm -rf -- "$probe_root" 2>/dev/null || true; fi
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
probe_root="$(mktemp -d "$slot_root/${kind}-${nonce}.XXXXXX" 2>/dev/null || true)"
if [ -z "$probe_root" ] || [ ! -d "$probe_root" ]; then
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
        root="$HOME/'"$AGENT_ROOT"'"
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
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --idle-seconds) REAP_IDLE="${2:-}"; shift 2 ;;
            --dry-run) REAP_DRY=1; shift ;;
            --authorized-only) REAP_AUTHORIZED_ONLY=1; shift ;;
            --eligible-key)
                _eligible_key="${2:-}"
                case "$_eligible_key" in
                    ""|*[!A-Za-z0-9._-]*) die "--eligible-key must be a safe lane key" ;;
                esac
                REAP_ELIGIBLE="${REAP_ELIGIBLE}${REAP_ELIGIBLE:+,}${_eligible_key}"
                shift 2
                ;;
            *) die "unknown reap arg: $1" ;;
        esac
    done
    case "$REAP_IDLE" in
        *[!0-9]*|"") die "--idle-seconds must be a non-negative integer" ;;
    esac
    if [ "$REAP_DRY" -eq 0 ] && [ "$REAP_AUTHORIZED_ONLY" -eq 0 ] && [ -z "$REAP_ELIGIBLE" ]; then
        echo "remote_agent: no authoritative eligible-key allowlist; reap is observation-only" >&2
        REAP_DRY=1
    fi
    # The remote mutex serialises the entire check/act pass. Per-lane locks are
    # then held across the final lease/pid re-probe and deletion [CON-11].
    "${SSH[@]}" bash -s -- "$AGENT_ROOT" "$REAP_IDLE" "$REAP_DRY" \
        "$REAP_AUTHORIZED_ONLY:$REAP_ELIGIBLE:/tmp" <<'REAP_EOF'
set -euo pipefail
agent_root="$1"
idle_limit="$2"
dry_run="$3"
authorized_only="${4%%:*}"
_reap_scope="${4#*:}"
eligible_keys="${_reap_scope%:*}"
tmp_root="${_reap_scope##*:}"
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

# Fixed descriptors, not {var}> automatic allocation. This body normally runs
# on the Linux VM, but test_remote_agent_dispatch_reaper.py stubs ssh with
# `exec bash -s` and executes it under the HOST bash -- 3.2.57 on macOS, which
# has no {var}> and exits 127. Fixed fds work on both. Deliberately NOT fd 9:
# test_remote_agent_lane_lock.py locates the per-lane build lock by the FIRST
# `exec 9>` line in the file, so reusing 9 here would shadow it.
command -v flock >/dev/null 2>&1 || { echo 'remote_agent: reap_flock_unavailable: flock required on the VM; refusing to reap without a lock' >&2; exit 78; }
reap_lock_fd=7
exec 7>>"$ROOT/.reap.lock"
if ! flock -n "$reap_lock_fd"; then
    printf 'remote_agent: reap lock held: %s\n' "$ROOT/.reap.lock" >&2
    printf '{"reaped":0,"would_reap":0,"skipped_live":0,"skipped_locked":0,"bytes_freed":0,"would_free_bytes":0,"sandbox_count_after":%s,"uv_cache_bytes":0,"tmp_scratch_bytes":0,"tmp_scratch_reaped":0,"would_uv_cache_bytes":0,"would_tmp_scratch_bytes":0,"would_tmp_scratch_reaped":0}\n' "$(_sandbox_count)"
    exit 75
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

_lane_is_merged_marked() {
    _lk="${1:-}"
    [ -n "$_lk" ] || return 1
    _mf="$ROOT/.merged/${_lk}.json"
    [ -s "$_mf" ]
}

_PROC_CWD_SNAPSHOT=
_PROC_CWD_TABLE=
_PROC_CWD_SNAPSHOT_READY=0
_lane_proc_root() {
    printf '%s' "${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
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
    IFS= read -r _stat_line <"$_pr/$_pid/stat" 2>/dev/null || return 1
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
    [ -O "$_pr/$_pid" ] || return 1
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
        _snapshot_proc_cwds
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
    for _pid in $_pids; do
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
            env kill -s TERM "$_pid" 2>/dev/null || true
        fi
    done
    _waited=0
    while [ "$_waited" -lt "$_poll" ]; do
        _still=0
        for _pid in $_pids; do
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
        if _lane_pid_still_in_dir "$_pid" "$_dir"; then
            env kill -s KILL "$_pid" 2>/dev/null || true
        fi
    done
    for _pid in $_pids; do
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
    while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            env kill -s TERM "$_pid" 2>/dev/null || true
        fi
    done <<< "$_records"
    _waited=0
    while [ "$_waited" -lt "$_poll" ]; do
        _still=0
        while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
            [ -n "$_pid" ] || continue
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
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            env kill -s KILL "$_pid" 2>/dev/null || true
        fi
    done <<< "$_records"
    while IFS=$'\t' read -r _pid _target _start || [ -n "${_pid:-}" ]; do
        [ -n "$_pid" ] || continue
        if _lane_pid_still_in_dir "$_pid" "" "$_target" "$_start"; then
            return 1
        fi
    done <<< "$_records"
    return 0
}

_lane_stop_processes() {
    _key="${1:-}"
    _dir="${2:-}"
    [ -n "$_key" ] && [ -n "$_dir" ] || return 1
    case "$_dir" in
        "$ROOT"|"$ROOT"/*) ;;
        *) return 1 ;;
    esac
    timeout 20 systemctl --user stop "grok-lane-${_key}.scope" 2>/dev/null || true
    timeout 20 systemctl --user stop "grok-lane-${_key}-sv.scope" 2>/dev/null || true
    _job_list="$(timeout 20 systemctl --user list-units --plain --no-legend "grok-lane-${_key}-job-*" 2>/dev/null || true)"
    if [ -n "$_job_list" ]; then
        _old_ifs="$IFS"
        IFS=$'\n'
        for _job_line in $_job_list; do
            IFS="$_old_ifs"
            _job_unit="${_job_line%% *}"
            [ -n "$_job_unit" ] || continue
            timeout 20 systemctl --user stop "$_job_unit" 2>/dev/null || true
        done
        IFS="$_old_ifs"
    fi
    _pids="$(_lane_collect_matching_pids sandbox "$_dir")"
    # shellcheck disable=SC2086
    _lane_terminate_pid_list "$_dir" $_pids
}

_lane_reap_deleted_cwd_orphans() {
    _records="$(_lane_collect_matching_pids deleted_root)"
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

_snapshot_proc_cwds() {
    _PROC_CWD_SNAPSHOT=""
    _PROC_CWD_TABLE=""
    _PROC_CWD_SNAPSHOT_READY=1
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    _pid_dirs=()
    _cwds=()
    _pids=()
    _n_pids=0
    for _cwd in "$_pr"/[0-9]*/cwd; do
        [ -L "$_cwd" ] || continue
        _p="${_cwd%/cwd}"
        _pid=""
        if [[ "$_p" =~ /([0-9]+)$ ]]; then
            _pid="${BASH_REMATCH[1]}"
        fi
        case "$_pid" in ''|*[!0-9]*) continue ;; esac
        _pid_dirs+=("$_p")
        _cwds+=("$_cwd")
        _pids+=("$_pid")
        _n_pids=$((_n_pids + 1))
    done
    if [ "$_n_pids" -eq 0 ]; then
        return 0
    fi
    _uids=()
    _targets=()
    while IFS= read -r _uid || [ -n "$_uid" ]; do
        _uids+=("$_uid")
    done < <(stat -c %u -- "${_pid_dirs[@]}" 2>/dev/null || true)
    while IFS= read -r _target || [ -n "$_target" ]; do
        _targets+=("$_target")
    done < <(readlink -- "${_cwds[@]}" 2>/dev/null || true)
    _i=0
    while [ "$_i" -lt "$_n_pids" ]; do
        _pid="${_pids[$_i]}"
        _uid="${_uids[$_i]:-}"
        _target="${_targets[$_i]:-}"
        _start=
        _lane_pid_start_identity "$_pid" || true
        _start="${_LANE_PID_START_IDENTITY:-}"
        _PROC_CWD_SNAPSHOT="${_PROC_CWD_SNAPSHOT}${_target}"$'\n'
        _PROC_CWD_TABLE="${_PROC_CWD_TABLE}${_pid}"$'\t'"${_uid}"$'\t'"${_target}"$'\t'"${_start}"$'\n'
        _i=$((_i + 1))
    done
}

_pid_in_sandbox_live() {
    _sandbox="$1"
    [ -n "$_sandbox" ] || return 1
    _pr="${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
    _cwds=()
    _n_cwds=0
    for _cwd in "$_pr"/[0-9]*/cwd; do
        [ -L "$_cwd" ] || continue
        _cwds+=("$_cwd")
        _n_cwds=$((_n_cwds + 1))
    done
    if [ "$_n_cwds" -eq 0 ]; then
        return 1
    fi
    while IFS= read -r _target || [ -n "${_target:-}" ]; do
        [ -n "$_target" ] || continue
        case "$_target" in
            "$_sandbox"|"$_sandbox"/*|"$_sandbox (deleted)"|"$_sandbox/"*" (deleted)") return 0 ;;
        esac
    done < <(readlink -- "${_cwds[@]}" 2>/dev/null || true)
    return 1
}

_pid_in_sandbox() {
    _sandbox="$1"
    [ -n "$_sandbox" ] || return 1
    if [ "${_PROC_CWD_SNAPSHOT_READY:-0}" -eq 1 ]; then
        while IFS= read -r _target || [ -n "$_target" ]; do
            [ -n "$_target" ] || continue
            case "$_target" in
                "$_sandbox"|"$_sandbox"/*|"$_sandbox (deleted)"|"$_sandbox/"*" (deleted)") return 0 ;;
            esac
        done <<< "$_PROC_CWD_SNAPSHOT"
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
tmp_scratch_skipped_unnamed=0
tmp_scratch_skipped_no_owner=0
tmp_scratch_skipped_live=0
tmp_scratch_skipped_fresh=0
tmp_scratch_skipped_unmeasurable=0
tmp_scratch_skipped_foreign_owner=0
now=$(date +%s)
reap_budget="${REAP_BUDGET_SEC:-90}"
case "$reap_budget" in *[!0-9]*|"") reap_budget=90 ;; esac
reap_deadline=$((now + reap_budget))
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
_snapshot_proc_cwds
candidates=0
_merged_keys=""
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
            *) continue ;;
        esac
    fi
    candidates=$((candidates + 1))
    _glob_keys="${_glob_keys} ${_key}"
    if _lane_is_merged_marked "$_key"; then
        _merged_keys="${_merged_keys} ${_key}"
    else
        _mtime=$(stat -c %Y "$_marker" 2>/dev/null || echo 0)
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
        exec 8>&-
        continue
    fi
    # Strong liveness signals outrank every clock. Malformed leases fail safe.
    # A live lease always wins over a merged marker. Exclusive flock above is
    # the lock-held proof for this authorized path (the dispatch body probes
    # `_lane_lock_held` without taking the lock).
    if _lane_occupant_live "$_key"; then
        skipped_live=$((skipped_live + 1))
        flock -u "$lane_lock_fd" || true
        exec 8>&-
        continue
    fi
    _mtime=$(stat -c %Y "$_marker" 2>/dev/null || true)
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
            printf '{"key":"%s","outcome":"kept_live_pid","reason":"sandbox_idle","age":%s,"actor":"maintenance","at":%s}\n' \
                "$_key" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: kept_live_pid journal failed for $_key" >&2
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
    _before=$(du -sb "$_sd" "$_venv" "$_stamp" 2>/dev/null | awk '{sum += $1} END {print sum + 0}' || true)
    case "$_before" in ''|*[!0-9]*) _before=0 ;; esac
    would_reap=$((would_reap + 1))
    would_free_bytes=$((would_free_bytes + _before))
    if [ "$dry_run" -eq 0 ]; then
        _reason=sandbox_idle
        [ "$_merged" -eq 1 ] && _reason=sandbox_merged
        if [ "$_merged" -eq 1 ]; then
            if ! _lane_stop_processes "$_key" "$_sd"; then
                skipped_live=$((skipped_live + 1))
                printf '{"key":"%s","outcome":"kept_process_survived","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
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
                printf '{"key":"%s","outcome":"kept_late_pid","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: kept_late_pid journal failed for $_key" >&2
                flock -u "$lane_lock_fd" || true
                exec 8>&-
                continue
            fi
        else
            # CON-11: re-probe the TTL candidate immediately before unlink.
            # Do not terminate on the TTL-only path.
            if _pid_in_sandbox_live "$_sd"; then
                skipped_live=$((skipped_live + 1))
                printf '{"key":"%s","outcome":"kept_live_pid","reason":"%s","age":%s,"actor":"maintenance","at":%s}\n' \
                    "$_key" "$_reason" "$_age" "$(date +%s)" >>"$ROOT/.reap-outcomes.jsonl" || \
                    echo "remote_agent: kept_live_pid journal failed for $_key" >&2
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
orphans_killed=0
if [ "$dry_run" -eq 0 ]; then
    orphans_killed="$(_lane_reap_deleted_cwd_orphans)"
    case "$orphans_killed" in ''|*[!0-9]*) orphans_killed=0 ;; esac
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
    _scratch_mtimes="$(find -P "$_scratch_dir" \
        ! -name '.workbay-scratch' \
        ! -name 'owner.pid' \
        ! -name '.owner.pid' \
        ! -name '.workbay-owner.pid' \
        -exec stat -c %Y {} \; 2>/dev/null)" || return 1
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
# `.reap-outcomes.jsonl`. That file is deletion-reconciliation evidence, and on
# a box with ~90 unrecognized /tmp entries a per-pass journal row for each one
# would add ~1k rows/day of pure noise to the record that has to stay readable
# after a bad sweep. The durable, machine-readable fact is the COUNT, which
# rides in the receipt; the paths are an operator-facing diagnostic.
_unnamed_report_cap=20

# Classify one candidate's terminal disposition. Callers MUST pair this with
# `continue`; it cannot break the loop itself. An unknown bucket is loud rather
# than silently dropped, because a typo here would recreate the original defect
# in a subtler form: a candidate that is examined but accounted nowhere.
_scratch_skip() {
    case "$1" in
        unnamed) tmp_scratch_skipped_unnamed=$((tmp_scratch_skipped_unnamed + 1)) ;;
        no_owner) tmp_scratch_skipped_no_owner=$((tmp_scratch_skipped_no_owner + 1)) ;;
        live) tmp_scratch_skipped_live=$((tmp_scratch_skipped_live + 1)) ;;
        fresh) tmp_scratch_skipped_fresh=$((tmp_scratch_skipped_fresh + 1)) ;;
        unmeasurable) tmp_scratch_skipped_unmeasurable=$((tmp_scratch_skipped_unmeasurable + 1)) ;;
        foreign_owner) tmp_scratch_skipped_foreign_owner=$((tmp_scratch_skipped_foreign_owner + 1)) ;;
        *) echo "remote_agent: scratch_skip_bucket_unknown: $1" >&2 ;;
    esac
    return 0
}

for _scratch_path in "$tmp_root"/*/; do
    [ "$(date +%s)" -lt "$reap_deadline" ] || break
    [ -d "$_scratch_path" ] || continue
    _scratch="${_scratch_path%/}"
    [ "$_scratch" = "$uv_tmp_root" ] && continue
    [ ! -L "$_scratch" ] || continue
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
        _scratch_uid="$(stat -c %u -- "$_scratch" 2>/dev/null || true)"
        case "$_scratch_uid" in
            ''|*[!0-9]*)
                _scratch_skip unmeasurable
                continue
                ;;
        esac
        if [ "$_scratch_uid" != "$_reap_uid" ]; then
            _scratch_skip foreign_owner
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
        _scratch_skip unnamed
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
        done <"$_scratch_marker" || { _scratch_skip no_owner; continue; }
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
        ""|*[!A-Za-z0-9._-]*) _scratch_skip no_owner; continue ;;
    esac
    _scratch_has_owner=0
    case "$_scratch_pid" in
        ''|0|*[!0-9]*)
            if [ ! -f "$ROOT/.lane-live-$_scratch_key" ]; then
                _scratch_skip no_owner
                continue
            fi
            ;;
        *) _scratch_has_owner=1 ;;
    esac
    [ -f "$ROOT/.lane-live-$_scratch_key" ] && _scratch_has_owner=1
    if [ "$_scratch_has_owner" -ne 1 ]; then
        _scratch_skip no_owner
        continue
    fi
    if _scratch_pid_live "$_scratch_pid" || _lane_occupant_live "$_scratch_key" || _pid_in_sandbox "$_scratch"; then
        _scratch_skip live
        continue
    fi
    # A tree whose newest mtime cannot be derived is unmeasurable, not idle:
    # refuse it, but say so rather than folding it into the same bare zero.
    _scratch_mtime="$(_scratch_newest_mtime "$_scratch")" || {
        _scratch_skip unmeasurable
        continue
    }
    case "$_scratch_mtime" in ''|*[!0-9]*) _scratch_skip unmeasurable; continue ;; esac
    _scratch_age=$((now - _scratch_mtime))
    if [ "$_scratch_age" -le "$idle_limit" ]; then
        _scratch_skip fresh
        continue
    fi

    _scratch_lock="$ROOT/.lane-lock-$_scratch_key"
    if [ -L "$_scratch_lock" ]; then
        _scratch_skip unmeasurable
        continue
    fi
    scratch_lock_fd=8
    if ! exec 8>>"$_scratch_lock"; then
        _scratch_skip unmeasurable
        continue
    fi
    # Lock contention is a live writer, not an absence of candidates.
    if ! flock -n "$scratch_lock_fd"; then
        exec 8>&-
        _scratch_skip live
        continue
    fi
    scratch_local_lock_fd=6
    scratch_local_lock=
    if [ -e "$_scratch/.lock" ]; then
        [ ! -L "$_scratch/.lock" ] || {
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip unmeasurable
            continue
        }
        scratch_local_lock="$_scratch/.lock"
        if ! exec 6>>"$scratch_local_lock" || ! flock -n "$scratch_local_lock_fd"; then
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            exec 6>&-
            _scratch_skip live
            continue
        fi
    fi
    # Re-probe all ownership/liveness signals under the writer lock [CON-11].
    if _scratch_pid_live "$_scratch_pid" || _lane_occupant_live "$_scratch_key" || _pid_in_sandbox "$_scratch"; then
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip live
        continue
    fi
    _scratch_mtime="$(_scratch_newest_mtime "$_scratch")" || _scratch_mtime=
    case "$_scratch_mtime" in
        ''|*[!0-9]*)
            [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
            flock -u "$scratch_lock_fd" || true
            exec 8>&-
            _scratch_skip unmeasurable
            continue
            ;;
    esac
    _scratch_age=$((now - _scratch_mtime))
    if [ "$_scratch_age" -le "$idle_limit" ]; then
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip fresh
        continue
    fi
    _scratch_before="$(du -sb "$_scratch" 2>/dev/null | awk '{sum += $1} END {print sum + 0}' || true)"
    case "$_scratch_before" in ''|*[!0-9]*)
        [ -n "$scratch_local_lock" ] && { flock -u "$scratch_local_lock_fd" || true; exec 6>&-; }
        flock -u "$scratch_lock_fd" || true
        exec 8>&-
        _scratch_skip unmeasurable
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
cd -- "$_reap_cwd"
sandbox_count_after=$(_sandbox_count)
printf '{"reaped":%s,"would_reap":%s,"skipped_live":%s,"skipped_locked":%s,"bytes_freed":%s,"would_free_bytes":%s,"sandbox_count_after":%s,"uv_cache_bytes":%s,"tmp_scratch_bytes":%s,"tmp_scratch_reaped":%s,"would_uv_cache_bytes":%s,"would_tmp_scratch_bytes":%s,"would_tmp_scratch_reaped":%s,"tmp_scratch_candidates_seen":%s,"tmp_scratch_skipped_unnamed":%s,"tmp_scratch_skipped_no_owner":%s,"tmp_scratch_skipped_live":%s,"tmp_scratch_skipped_fresh":%s,"tmp_scratch_skipped_unmeasurable":%s,"tmp_scratch_skipped_foreign_owner":%s}\n' \
    "$reaped" "$would_reap" "$skipped_live" "$skipped_locked" "$bytes_freed" "$would_free_bytes" "$sandbox_count_after" \
    "$uv_cache_bytes" "$tmp_scratch_bytes" "$tmp_scratch_reaped" "$would_uv_cache_bytes" "$would_tmp_scratch_bytes" "$would_tmp_scratch_reaped" \
    "$tmp_scratch_candidates_seen" "$tmp_scratch_skipped_unnamed" "$tmp_scratch_skipped_no_owner" \
    "$tmp_scratch_skipped_live" "$tmp_scratch_skipped_fresh" "$tmp_scratch_skipped_unmeasurable" \
    "$tmp_scratch_skipped_foreign_owner"
REAP_EOF
    ;;
status)
    exec </dev/null
    STATUS_SWEEP=0
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
            *) die "unknown arg: $1" ;;
        esac
    done
    if [ -z "$STATUS_JOB_IDS" ] && [ "$STATUS_SWEEP" != 1 ]; then
        die "--job-id or --sweep required"
    fi
    _JOB_OPS_SRC="$(declare -f _remote_job_ops)"
    _status_rc=0
    # shellcheck disable=SC2029
    "${SSH[@]}" env \
        "AGENT_ROOT=${AGENT_ROOT}" \
        "STATUS_JOB_IDS=${STATUS_JOB_IDS}" \
        "STATUS_SWEEP=${STATUS_SWEEP}" \
        "REMOTE_JOB_OP=status" \
        bash -s <<STATUS_EOF || _status_rc=$?
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
    _JOB_OPS_SRC="$(declare -f _remote_job_ops)"
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
    if ! tar -C "${_collect_tmp}/out" -xf "${_collect_tmp}/bundle.tar"; then
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
    cat "${_collect_tmp}/out/stdout"
    cat "${_collect_tmp}/out/stderr" >&2
    _want_sha="$(printf '%s\n' "$_want" | sed -n '1p')"
    _recorded_rc="$(printf '%s\n' "$_want" | sed -n '2p')"
    _got="$(sha256sum "${_collect_tmp}/out/stdout" 2>/dev/null | awk '{print $1}')"
    if [ -z "$_want_sha" ] || [ "$_want_sha" != "$_got" ]; then
        echo "remote_agent: stdout_sha256_mismatch" >&2
        rm -rf "$_collect_tmp"
        exit 5
    fi
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
    _JOB_OPS_SRC="$(declare -f _remote_job_ops)"
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
    JOB_ID=""
    AGENT_SPEC_ENABLED=0
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="/dev/null" AGENT_SPEC_STDOUT="{result_file}"
    AGENT_SPEC_STDERR="{run_log}" AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="9"
    AGENT_SPEC_ENV_FILE="" AGENT_SPEC_BRANCH="" AGENT_SPEC_HEAD_SHA=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
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
_out.close()
PY
    then
        rm -f "$_meta_tmp"
        die "--agent-spec metadata rejected (see stderr)"
    fi
    # Fixed-position NUL load (same discipline as the argv sidecar). 10 fields.
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="" AGENT_SPEC_STDOUT="" AGENT_SPEC_STDERR=""
    AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="" AGENT_SPEC_REQUIRES_TIMEOUT=""
    AGENT_SPEC_ENV_FILE="" AGENT_SPEC_BRANCH="" AGENT_SPEC_HEAD_SHA=""
    _meta_i=0
    while IFS= read -r -d '' _mv; do
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
            *)
                rm -f "$_meta_tmp"
                die "--agent-spec metadata: too many fields"
                ;;
        esac
        _meta_i=$((_meta_i + 1))
    done < "$_meta_tmp"
    rm -f "$_meta_tmp"
    [ "$_meta_i" -eq 10 ] || die "--agent-spec metadata field count ${_meta_i} (need 10)"
    [ -n "${AGENT_SPEC_BIN:-}" ] || die "--agent-spec missing binary"
    AGENT_SPEC_ENABLED=1
    case "$BRANCH" in *[!A-Za-z0-9/_.-]*) die "unsafe --branch name" ;; esac
    case "$TIMEOUT" in *[!0-9]*|"") die "--timeout must be a non-negative integer (seconds; 0=none)" ;; esac
    # implementation note S6 / R2-H08: requires_timeout backends refuse --timeout 0 (exit 7).
    if [ "${AGENT_SPEC_REQUIRES_TIMEOUT:-0}" = 1 ] && [ "$TIMEOUT" -le 0 ]; then
        echo "remote_agent: policy refused: requires_timeout but --timeout is ${TIMEOUT} (need positive bound)" >&2
        exit 7
    fi
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
    # Named systemd scope unit: grok-lane-<LANE_KEY>.scope so active lanes are
    # countable + debuggable ([RES-14] concurrency ceiling; implementation note S5).
    # Suffix is part of the name: is-active/reset-failed resolve bare names to
    # .service, but the occupant is created as a scope (systemd-run --scope).
    LANE_UNIT="grok-lane-${LANE_KEY}.scope"
    LANE_UNIT_SV="grok-lane-${LANE_KEY}-sv.scope"

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
        if ! "${SSH[@]}" "_lane_occupant_live() { \
                _lk=\"\${1:-}\"; \
                [ -n \"\$_lk\" ] || return 0; \
                _lf=\"\$HOME/${AGENT_ROOT}/.lane-live-\$_lk\"; \
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
            _lv=\"\$HOME/${AGENT_ROOT}/.lane-live-${LANE_KEY}\"; \
            _live=0; \
            if [ -f \"\$_lv\" ] && grep -qx 'nonce=${DISPATCH_NONCE}' \"\$_lv\" 2>/dev/null; then \
                if _lane_occupant_live \"${LANE_KEY}\"; then _live=1; fi; \
            fi; \
            _handoff=0; \
            _cleanup=1; \
            if [ \"\$_live\" != 1 ] && [ -n \"${JOB_ID:-}\" ]; then \
                _jd=\"\$HOME/${AGENT_ROOT}/.jobs/${JOB_ID:-}\"; \
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
                \"\$HOME/${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md\" \
                \"\$HOME/${AGENT_ROOT}/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-result.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-debug.log\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-selfverify.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-phases.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.workbay-source-provenance.json\" \
                \"\$HOME/${AGENT_ROOT}/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json\" \
                \"\$HOME/${AGENT_ROOT}/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.argv\" \
                2>/dev/null; \
                rmdir \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}\" 2>/dev/null; \
                git -C \"\$HOME/${REMOTE_DIR}\" update-ref -d 'refs/heads/${LANE_KEY}-${DISPATCH_NONCE}' 2>/dev/null; \
            fi; \
            true"; then
            echo 'remote_agent: dispatch transient cleanup failed (non-fatal)' >&2
        fi
        return 0
    }
    trap '_cleanup_dispatch_transients' EXIT

    # Single-source admission (MemAvailable floor + lane cap) used by both the
    # PRE-dispatch probe and the in-run TOCTOU re-check so the two sites cannot
    # drift [REF-10]. Fail-open probe glitches to admit (avail_mb=0 /
    # active_lanes=0). Lane-count never double-emits under pipefail (systemctl
    # fail + awk print + `|| echo 0` used to yield "0\n0" and break integer
    # compare). Placeholders __MEM_FLOOR_MB__ / __MAX_LANES__ are substituted
    # with validated integers only.
    read -r -d '' _admission_tpl <<'ADMISSION_EOF' || true
avail_mb=$(awk '/^MemAvailable:/{printf "%d", $2/1024}' /proc/meminfo 2>/dev/null || echo 0)
if [ "$avail_mb" -gt 0 ] && [ "$avail_mb" -lt __MEM_FLOOR_MB__ ]; then
    echo "remote_agent: VM MemAvailable ${avail_mb}MiB < __MEM_FLOOR_MB__MiB floor — deferring lane (non-lane work has priority)" >&2
    exit 75
fi
active_lanes=0
if _al_out=$(systemctl --user list-units --type=scope --state=active --plain --no-legend 'grok-lane-*' 2>/dev/null | awk 'END{print NR+0}'); then
    case "$_al_out" in ''|*[!0-9]*) active_lanes=0 ;; *) active_lanes=$_al_out ;; esac
fi
if [ "$active_lanes" -ge __MAX_LANES__ ]; then
    echo "remote_agent: lane cap __MAX_LANES__ reached — deferring" >&2
    exit 75
fi
# Pid dimension. systemd charges *threads* to pids.max, so the slice can be
# exhausted while MemAvailable and the lane-scope count both read healthy —
# and the entitlement probe runs outside any grok-lane-* scope, so it consumes
# budget the lane counter cannot see. Fails open on any unreadable/odd value
# (pids_max=0 skips), matching the two dimensions above.
pids_root="${WORKBAY_REMOTE_GATE_PIDS_ROOT:-/sys/fs/cgroup/user.slice/user-$(id -u).slice}"
pids_max=$(cat "$pids_root/pids.max" 2>/dev/null || echo 0)
pids_cur=$(cat "$pids_root/pids.current" 2>/dev/null || echo 0)
case "$pids_max" in ''|*[!0-9]*) pids_max=0 ;; esac
case "$pids_cur" in ''|*[!0-9]*) pids_cur=0 ;; esac
if [ "$pids_max" -gt 0 ] && [ __PID_FLOOR__ -gt 0 ]; then
    pids_free=$(( pids_max - pids_cur ))
    if [ "$pids_free" -lt __PID_FLOOR__ ]; then
        echo "remote_agent: vm_pid_saturation — ${pids_free} free pids < __PID_FLOOR__ floor (cgroup ${pids_cur}/${pids_max} at ${pids_root}) — deferring lane" >&2
        exit 75
    fi
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
    _admission_remote_sh="${_admission_tpl//__MEM_FLOOR_MB__/${MEM_FLOOR_MB}}"
    _admission_remote_sh="${_admission_remote_sh//__MAX_LANES__/${MAX_LANES}}"
    _admission_remote_sh="${_admission_remote_sh//__PID_FLOOR__/${PID_FLOOR}}"
    _admission_remote_sh="${_admission_remote_sh//__AGENT_ROOT__/${AGENT_ROOT}}"

    # 0) PRE-dispatch admission probe BEFORE any transfer cost. Exit 75 is the
    # same retryable-defer contract as the in-run check (TOCTOU belt-and-
    # suspenders — keep both).
    # shellcheck disable=SC2029
    "${SSH[@]}" bash -s <<REMOTE_EOF >&2
set -euo pipefail
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
    "${SSH[@]}" "mkdir -p \"\$HOME/${AGENT_ROOT}\"" >&2
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
    if [ -n "\$_PHASES_JSON_PARTS" ]; then
        _PHASES_JSON_PARTS="\${_PHASES_JSON_PARTS},\${_ph_frag}"
    else
        _PHASES_JSON_PARTS="\$_ph_frag"
    fi
}
# Fail-open write of OUT_DIR/.grok-phases.json (never stdout; never fatal).
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
    _phases_path="\$OUT_DIR/.grok-phases.json"
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
ROOT="\$HOME/${AGENT_ROOT}"
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
# Absent flock, the '||' below would fire on command-not-found and defer EVERY
# lane forever under a misleading "already active" message. Fail loud instead
# [AGT-10]: the guard is not optional, so a host without flock is misconfigured
# (78), not busy (75). Linux VMs have it; remote_gate.sh already depends on it.
command -v flock >/dev/null 2>&1 || { echo 'remote_agent: flock unavailable on the VM — refusing to run without the same-branch collision guard' >&2; exit 78; }
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
# Bounded disk growth (internal S4): persisted lane venvs
# accumulate one per distinct branch.  Eviction takes the candidate's lane lock
# and revalidates its occupancy lease while holding that lock.  Thus another
# lane can neither start using the interpreter nor publish a live lease between
# the liveness check and deletion.  Probe/lock errors spare the candidate.
_lane_lru_reap_candidate() {
    _old="\${1:-}"
    _oldkey="\${_old##*/.venv-lane-}"
    [ -n "\$_old" ] && [ -n "\$_oldkey" ] || return 0
    (
        exec 8>>"\$ROOT/.lane-lock-\${_oldkey}" || exit 0
        flock -n 8 || exit 0
        if _lane_occupant_live "\$_oldkey"; then
            exit 0
        fi
        _mt=\$(stat -c %Y "\$_old" 2>/dev/null || true)
        _now=\$(date +%s)
        _age=0
        case "\$_mt" in
            ''|*[!0-9]*) ;;
            *) _age=\$((_now - _mt)) ;;
        esac
        _before=\$(du -sb "\$_old" "\$ROOT/.venv-sync-stamp-\${_oldkey}" 2>/dev/null | awk '{sum += \$1} END {print sum + 0}' || true)
        case "\$_before" in ''|*[!0-9]*) _before=0 ;; esac
        if ! printf '{"key":"%s","outcome":"started","reason":"lru_venv","age":%s,"actor":"dispatch_lru","bytes_before":%s,"at":%s}\n' \
            "\$_oldkey" "\$_age" "\$_before" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl"; then
            echo "remote_agent: cannot persist lru reap intent for \$_oldkey; deletion refused" >&2
            exit 0
        fi
        rm -rf "\$_old" "\$ROOT/.venv-sync-stamp-\${_oldkey}" 2>/dev/null || true
        if [ ! -e "\$_old" ]; then
            printf '{"key":"%s","outcome":"reaped","reason":"lru_venv","age":%s,"actor":"dispatch_lru","bytes_freed":%s,"at":%s}\n' \
                "\$_oldkey" "\$_age" "\$_before" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: lru reap completion journal failed for \$_oldkey" >&2
        else
            printf '{"key":"%s","outcome":"incomplete","reason":"lru_venv","age":%s,"actor":"dispatch_lru","at":%s}\n' \
                "\$_oldkey" "\$_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                echo "remote_agent: lru incomplete reap journal failed for \$_oldkey" >&2
        fi
    )
}
if [ '${LANE_VENV_CAP}' -gt 0 ] 2>/dev/null; then
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
    if [ -z "\$_n" ]; then
        echo "remote_agent: bound runner requires a residual budget — refusing empty RuntimeMaxSec (unbounded)" >&2
        return 1
    fi
    if [ "\$_SCOPE_OK" = 1 ]; then
        if [ "\$_BOUND_MODE" = scope ] || [ "\$_BOUND_MODE" = ceiling ]; then
            printf '%s\n' "systemd-run --quiet --user --scope --unit ${LANE_UNIT} -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA} -p RuntimeMaxSec=\$_n nice -n 10 ionice -c3"
        else
            printf '%s\n' "systemd-run --quiet --user --scope --unit ${LANE_UNIT} -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA} nice -n 10 ionice -c3"
        fi
    elif [ "\$_BOUND_MODE" = scope ] || [ "\$_BOUND_MODE" = ceiling ]; then
        # MemoryMax unsupported; RuntimeMaxSec available — keep the process
        # bound, drop MemoryMax/CPUQuota (this host rejects them).
        printf '%s\n' "systemd-run --quiet --user --scope --unit ${LANE_UNIT} -p RuntimeMaxSec=\$_n nice -n 10 ionice -c3"
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
                if [ "\$_mtime_rc" -ne 0 ] && _dispatch_sweep_remaining; then
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
        _lane_proc_root() {
            printf '%s' "\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
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
            IFS= read -r _stat_line <"\$_pr/\$_pid/stat" 2>/dev/null || return 1
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
            [ -O "\$_pr/\$_pid" ] || return 1
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
                _snapshot_proc_cwds
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
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                if _remaining="\$(_dispatch_sweep_remaining)"; then
                    [ "\$_remaining" -lt "\$_poll" ] && _poll="\$_remaining"
                else
                    return 1
                fi
            fi
            for _pid in \$_pids; do
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                    env kill -s TERM "\$_pid" 2>/dev/null || true
                fi
            done
            _waited=0
            while [ "\$_waited" -lt "\$_poll" ]; do
                _still=0
                for _pid in \$_pids; do
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
                if _lane_pid_still_in_dir "\$_pid" "\$_dir"; then
                    env kill -s KILL "\$_pid" 2>/dev/null || true
                fi
            done
            for _pid in \$_pids; do
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
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                if _remaining="\$(_dispatch_sweep_remaining)"; then
                    [ "\$_remaining" -lt "\$_poll" ] && _poll="\$_remaining"
                else
                    return 1
                fi
            fi
            while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    env kill -s TERM "\$_pid" 2>/dev/null || true
                fi
            done <<< "\$_records"
            _waited=0
            while [ "\$_waited" -lt "\$_poll" ]; do
                _still=0
                while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                    [ -n "\$_pid" ] || continue
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
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    env kill -s KILL "\$_pid" 2>/dev/null || true
                fi
            done <<< "\$_records"
            while IFS=\$'\t' read -r _pid _target _start || [ -n "\${_pid:-}" ]; do
                [ -n "\$_pid" ] || continue
                if _lane_pid_still_in_dir "\$_pid" "" "\$_target" "\$_start"; then
                    return 1
                fi
            done <<< "\$_records"
            return 0
        }

        _lane_stop_processes() {
            _key="\${1:-}"
            _dir="\${2:-}"
            [ -n "\$_key" ] && [ -n "\$_dir" ] || return 1
            case "\$_dir" in
                "\$ROOT"|"\$ROOT"/*) ;;
                *) return 1 ;;
            esac
            _stop_timeout=20
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                if _remaining="\$(_dispatch_sweep_remaining)"; then
                    [ "\$_remaining" -lt "\$_stop_timeout" ] && _stop_timeout="\$_remaining"
                else
                    return 1
                fi
            fi
            timeout "\$_stop_timeout" systemctl --user stop "grok-lane-\${_key}.scope" 2>/dev/null || true
            timeout "\$_stop_timeout" systemctl --user stop "grok-lane-\${_key}-sv.scope" 2>/dev/null || true
            _job_list="\$(timeout "\$_stop_timeout" systemctl --user list-units --plain --no-legend "grok-lane-\${_key}-job-*" 2>/dev/null || true)"
            if [ -n "\$_job_list" ]; then
                _old_ifs="\$IFS"
                IFS=\$'\n'
                for _job_line in \$_job_list; do
                    IFS="\$_old_ifs"
                    _job_unit="\${_job_line%% *}"
                    [ -n "\$_job_unit" ] || continue
                    timeout "\$_stop_timeout" systemctl --user stop "\$_job_unit" 2>/dev/null || true
                done
                IFS="\$_old_ifs"
            fi
            _pids="\$(_lane_collect_matching_pids sandbox "\$_dir")"
            _lane_terminate_pid_list "\$_dir" \$_pids
        }
        _lane_reap_deleted_cwd_orphans() {
            _records="\$(_lane_collect_matching_pids deleted_root)"
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
        _snapshot_proc_cwds() {
            _PROC_CWD_SNAPSHOT=""
            _PROC_CWD_TABLE=""
            _PROC_CWD_SNAPSHOT_READY=1
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ] && ! _dispatch_sweep_remaining; then
                _PROC_CWD_SNAPSHOT_READY=0
                return 124
            fi
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            _pid_dirs=()
            _cwds=()
            _pids=()
            _n_pids=0
            for _cwd in "\$_pr"/[0-9]*/cwd; do
                [ -L "\$_cwd" ] || continue
                _p="\${_cwd%/cwd}"
                _pid=""
                if [[ "\$_p" =~ /([0-9]+)$ ]]; then
                    _pid="\${BASH_REMATCH[1]}"
                fi
                case "\$_pid" in ''|*[!0-9]*) continue ;; esac
                _pid_dirs+=("\$_p")
                _cwds+=("\$_cwd")
                _pids+=("\$_pid")
                _n_pids=\$((_n_pids + 1))
            done
            if [ "\$_n_pids" -eq 0 ]; then
                return 0
            fi
            _uids=()
            _targets=()
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                while IFS= read -r _uid || [ -n "\$_uid" ]; do
                    _uids+=("\$_uid")
                done < <(_dispatch_sweep_timeout stat -c %u -- "\${_pid_dirs[@]}" 2>/dev/null || true)
                while IFS= read -r _target || [ -n "\$_target" ]; do
                    _targets+=("\$_target")
                done < <(_dispatch_sweep_timeout readlink -- "\${_cwds[@]}" 2>/dev/null || true)
            else
                while IFS= read -r _uid || [ -n "\$_uid" ]; do
                    _uids+=("\$_uid")
                done < <(stat -c %u -- "\${_pid_dirs[@]}" 2>/dev/null || true)
                while IFS= read -r _target || [ -n "\$_target" ]; do
                    _targets+=("\$_target")
                done < <(readlink -- "\${_cwds[@]}" 2>/dev/null || true)
            fi
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ] && ! _dispatch_sweep_remaining; then
                _PROC_CWD_SNAPSHOT_READY=0
                return 124
            fi
            _i=0
            while [ "\$_i" -lt "\$_n_pids" ]; do
                _pid="\${_pids[\$_i]}"
                _uid="\${_uids[\$_i]:-}"
                _target="\${_targets[\$_i]:-}"
                _start=
                _lane_pid_start_identity "\$_pid" || true
                _start="\${_LANE_PID_START_IDENTITY:-}"
                _PROC_CWD_SNAPSHOT="\${_PROC_CWD_SNAPSHOT}\${_target}"\$'\n'
                _PROC_CWD_TABLE="\${_PROC_CWD_TABLE}\${_pid}"\$'\t'"\${_uid}"\$'\t'"\${_target}"\$'\t'"\${_start}"\$'\n'
                _i=\$((_i + 1))
            done
        }
        _pid_in_sandbox_live() {
            _sd="\${1:-}"
            [ -n "\$_sd" ] || return 0
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ] && ! _dispatch_sweep_remaining; then
                return 0
            fi
            _pr="\${WORKBAY_REMOTE_AGENT_PROC_ROOT:-/proc}"
            _cwds=()
            _n_cwds=0
            for _cwd in "\$_pr"/[0-9]*/cwd; do
                [ -L "\$_cwd" ] || continue
                _cwds+=("\$_cwd")
                _n_cwds=\$((_n_cwds + 1))
            done
            if [ "\$_n_cwds" -eq 0 ]; then
                return 1
            fi
            if [ -n "\${_DISPATCH_SWEEP_DEADLINE:-}" ]; then
                while IFS= read -r _target || [ -n "\${_target:-}" ]; do
                    [ -n "\$_target" ] || continue
                    case "\$_target" in
                        "\$_sd"|"\$_sd"/*|"\$_sd (deleted)"|"\$_sd/"*" (deleted)") return 0 ;;
                    esac
                done < <(_dispatch_sweep_timeout readlink -- "\${_cwds[@]}" 2>/dev/null || true)
            else
                while IFS= read -r _target || [ -n "\${_target:-}" ]; do
                    [ -n "\$_target" ] || continue
                    case "\$_target" in
                        "\$_sd"|"\$_sd"/*|"\$_sd (deleted)"|"\$_sd/"*" (deleted)") return 0 ;;
                    esac
                done < <(readlink -- "\${_cwds[@]}" 2>/dev/null || true)
            fi
            return 1
        }
        _pid_in_sandbox() {
            _sd="\${1:-}"
            [ -n "\$_sd" ] || return 0
            if [ "\${_PROC_CWD_SNAPSHOT_READY:-0}" -eq 1 ]; then
                while IFS= read -r _target || [ -n "\$_target" ]; do
                    [ -n "\$_target" ] || continue
                    case "\$_target" in
                        "\$_sd"|"\$_sd"/*|"\$_sd (deleted)"|"\$_sd/"*" (deleted)") return 0 ;;
                    esac
                done <<< "\$_PROC_CWD_SNAPSHOT"
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
        _now=\$(date +%s)
        # Bake own nonce at local dispatch (unquoted heredoc expands it into the
        # single-quoted literal). Remote never needs DISPATCH_NONCE set under -u.
        # Extract-only harnesses that leave the token unsubstituted get a no-op
        # self-exclusion (literal '\${DISPATCH_NONCE}' matches no real path).
        _self_nonce='${DISPATCH_NONCE}'
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
        # bytes_freed on reaped rows only. Return 0 iff reaped.
        _journal_deletion_outcome() {
            _jdo_key=\${1:-}
            _jdo_del=\${2:-1}
            _jdo_probe=\${3:-2}
            _jdo_reason=\${4:-}
            _jdo_age=\${5:-0}
            _jdo_bytes=\${6:-}
            if [ "\$_jdo_del" -eq 0 ] && [ "\$_jdo_probe" -eq 1 ]; then
                if [ -n "\$_jdo_bytes" ]; then
                    printf '{"key":"%s","outcome":"reaped","reason":"%s","age":%s,"actor":"dispatch_ttl","bytes_freed":%s,"at":%s}\n' \
                        "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$_jdo_bytes" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                        echo "remote_agent: ttl reap completion journal failed for \$_jdo_key" >&2
                else
                    printf '{"key":"%s","outcome":"reaped","reason":"%s","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                        "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
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
            printf '{"key":"%s","outcome":"incomplete","reason":"%s","age":%s,"actor":"dispatch_ttl","step":"%s","status":%s,"at":%s}\n' \
                "\$_jdo_key" "\$_jdo_reason" "\$_jdo_age" "\$_jdo_step" "\$_jdo_status" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
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
                    if [ -z "\$_olk" ] || _lane_occupant_live "\$_olk"; then
                        continue
                    fi
                    # Held lane lock outranks age (stronger than lease; no clock).
                    if _lane_lock_held "\$_olk"; then
                        continue
                    fi
                    ;;
            esac
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started dispatch_transient "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                continue
            fi
            _del_rc=0
            rm -rf "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" dispatch_transient "\$_age" || true
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
            # Live work wins over age and count. Recheck immediately before a
            # possible delete so a lease/lock written during the walk is safe.
            if _lane_occupant_live "\$_salvage_snapshot_lane" || \
               _lane_lock_held "\$_salvage_snapshot_lane"; then
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
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started salvage_snapshot "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                continue
            fi
            _del_rc=0
            rm -f "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            if _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" salvage_snapshot "\$_age"; then
                _salvage_snapshot_reclaimed=\$((_salvage_snapshot_reclaimed + 1))
            fi
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
            if _lane_occupant_live "\$_olk"; then
                _as_skip=\$((_as_skip + 1))
                _as_live=\$((_as_live + 1))
                continue
            fi
            if _lane_lock_held "\$_olk"; then
                _as_skip=\$((_as_skip + 1))
                _as_lock=\$((_as_lock + 1))
                continue
            fi
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_p" started agent_spec "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_p; deletion refused" >&2
                continue
            fi
            _del_rc=0
            rm -f "\$_p" 2>/dev/null || _del_rc=\$?
            _probe_rc=0
            [ -e "\$_p" ] || _probe_rc=\$?
            if _journal_deletion_outcome "\$_p" "\$_del_rc" "\$_probe_rc" agent_spec "\$_age"; then
                _as_rm=\$((_as_rm + 1))
            fi
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
            if [ -z "\$_olk" ] || _lane_occupant_live "\$_olk"; then
                continue
            fi
            # Held lane lock outranks age (stronger than lease; no clock).
            if _lane_lock_held "\$_olk"; then
                continue
            fi
            _rl="\$SRC/.git/logs/refs/heads/\$_bn"
            [ -f "\$_rl" ] || continue
            _reap_mtime "\$_rl" || continue
            [ "\$((_now - _mt))" -gt ${DISPATCH_TTL_SEC} ] || continue
            _age=\$((_now - _mt))
            if ! _ttl_outcome "\$_bn" started dispatch_ref "\$_age"; then
                echo "remote_agent: cannot persist ttl reap intent for \$_bn; deletion refused" >&2
                continue
            fi
            _del_rc=0
            git -C "\$SRC" update-ref -d "refs/heads/\$_bn" 2>/dev/null || _del_rc=\$?
            git -C "\$SRC" show-ref --verify --quiet "refs/heads/\$_bn" 2>/dev/null && _probe_rc=0 || _probe_rc=\$?
            _journal_deletion_outcome "\$_bn" "\$_del_rc" "\$_probe_rc" dispatch_ref "\$_age" || true
        done <<< "\$_ref_list"
        # Per-lane sandbox reaper [RES-07]: marker-gated age TTL for \$ROOT/<LANE_KEY>
        # worktrees and their .venv-lane-* / .venv-sync-stamp-* siblings. Only
        # directories this script marked are candidates — never a name heuristic
        # (archive-20260719 ends in 8 hex-valid chars). Own \$SBX and live leases
        # are excluded. 0 disables only this sweep; DISPATCH_TTL_SEC=0 is the
        # master off switch for the whole reaper block.
        if [ '${SANDBOX_TTL_SEC}' -gt 0 ] 2>/dev/null; then
            # Reclaim counters [OBS-08]: track deletions only (not candidates).
            # Report one stderr line when either is non-zero; quiet passes stay quiet.
            _reclaimed_sandboxes=0
            _reclaimed_orphan_venvs=0
            _sweep_reaped=0
            _sweep_freed_kb=0
            _sweep_candidates=0
            _sweep_budget_exhausted=0
            _sweep_started=\$(date +%s)
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
            _sweep_stamp="\$ROOT/.reap-last.json"
            _sweep_stamp_at=
            _sweep_run=1
            if [ "\$_sweep_pressured" -eq 0 ] && [ "\$_sweep_interval" -gt 0 ] && [ -s "\$_sweep_stamp" ]; then
                _sweep_stamp_json=\$(sed -n '1p' "\$_sweep_stamp" 2>/dev/null || true)
                case "\$_sweep_stamp_json" in
                    \{*\})
                        _sweep_stamp_at=\$(sed -n 's/.*"at"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "\$_sweep_stamp" 2>/dev/null | head -n1 || true)
                        ;;
                esac
                case "\$_sweep_stamp_at" in
                    ''|*[!0-9]*) ;;
                    *)
                        if [ "\$_sweep_stamp_at" -le "\$_sweep_started" ] && [ "\$(( _sweep_started - _sweep_stamp_at ))" -lt "\$_sweep_interval" ]; then
                            _sweep_run=0
                        fi
                        ;;
                esac
            fi
            if [ "\$_sweep_run" -eq 0 ]; then
                echo "remote_agent: sandbox sweep completed \${_sweep_stamp_at}s ago; skipping fresh pass" >&2
            else
                _DISPATCH_SWEEP_DEADLINE=\$(( _sweep_started + _sweep_budget ))
                _sweep_cursor_file="\$ROOT/.reap-sweep-cursor"
                _sweep_cursor=\$(sed -n '1p' "\$_sweep_cursor_file" 2>/dev/null || true)
                _sweep_keys=()
                if _snapshot_proc_cwds; then
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
                    _sweep_budget_exhausted=1
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
                    if ! _dispatch_sweep_remaining; then
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
                # Live-lane fail-safe: unexpired/malformed lease outranks age.
                if _lane_occupant_live "\$_sk"; then
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                # Held lane lock outranks age (stronger than lease; no clock).
                if _lane_lock_held "\$_sk"; then
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                # Age from the marker mtime (stable while work happens in subdirs).
                # A merged marker is immediately eligible; TTL remains the backstop.
                _reap_mtime "\$_sd/.workbay-lane-sandbox" || { _sweep_i=\$(( _sweep_i + 1 )); continue; }
                _age=\$(( \$(date +%s) - _mt ))
                if _lane_is_merged_marked "\$_sk"; then
                    if ! _lane_stop_processes "\$_sk" "\$_sd"; then
                        printf '{"key":"%s","outcome":"kept_process_survived","reason":"sandbox_merged","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                            "\$_sk" "\$_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                            echo "remote_agent: kept_process_survived journal failed for \$_sk" >&2
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                    # CON-11: a pid can enter the sandbox after the pre-loop
                    # snapshot (for example during systemctl stop). Re-probe
                    # live immediately before unlink. Distinct from
                    # kept_process_survived (a snapshot pid that outlived
                    # TERM/KILL).
                    if _pid_in_sandbox_live "\$_sd"; then
                        printf '{"key":"%s","outcome":"kept_late_pid","reason":"sandbox_merged","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                            "\$_sk" "\$_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                            echo "remote_agent: kept_late_pid journal failed for \$_sk" >&2
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                else
                    # A detached child can outlive both shell lock and lease.
                    # TTL-only path never terminates; it reports the skip.
                    if _pid_in_sandbox "\$_sd"; then
                        printf '{"key":"%s","outcome":"kept_live_pid","reason":"sandbox_ttl","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                            "\$_sk" "\$_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                            echo "remote_agent: kept_live_pid journal failed for \$_sk" >&2
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                    [ "\$(( \$(date +%s) - _mt ))" -gt ${SANDBOX_TTL_SEC} ] || { _sweep_i=\$(( _sweep_i + 1 )); continue; }
                    if _pid_in_sandbox_live "\$_sd"; then
                        printf '{"key":"%s","outcome":"kept_live_pid","reason":"sandbox_ttl","age":%s,"actor":"dispatch_ttl","at":%s}\n' \
                            "\$_sk" "\$_age" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl" || \
                            echo "remote_agent: kept_live_pid journal failed for \$_sk" >&2
                        _sweep_i=\$(( _sweep_i + 1 ))
                        continue
                    fi
                fi
                _before=\$(du -sb "\$_sd" "\$ROOT/.venv-lane-\$_sk" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null | awk '{sum += \$1} END {print sum + 0}' || true)
                case "\$_before" in ''|*[!0-9]*) _before=0 ;; esac
                if ! printf '{"key":"%s","outcome":"started","reason":"sandbox_ttl","age":%s,"actor":"dispatch_ttl","bytes_before":%s,"at":%s}\n' \
                    "\$_sk" "\$_age" "\$_before" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl"; then
                    echo "remote_agent: cannot persist ttl reap intent for \$_sk; deletion refused" >&2
                    _sweep_i=\$(( _sweep_i + 1 ))
                    continue
                fi
                _del_rc=0
                rm -rf "\$_sd" "\$ROOT/.venv-lane-\$_sk" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null || _del_rc=\$?
                _probe_rc=0
                [ -e "\$_sd" ] || _probe_rc=\$?
                if _journal_deletion_outcome "\$_sk" "\$_del_rc" "\$_probe_rc" sandbox_ttl "\$_age" "\$_before"; then
                    _reclaimed_sandboxes=\$((_reclaimed_sandboxes + 1))
                    _sweep_reaped=\$(( _sweep_reaped + 1 ))
                    _sweep_freed_kb=\$(( _sweep_freed_kb + _before / 1024 ))
                fi
                    _sweep_i=\$(( _sweep_i + 1 ))
                done
                if [ "\$_sweep_budget_exhausted" -eq 0 ]; then
                    if _dispatch_sweep_remaining; then
                        _orphans_killed=\$(_lane_reap_deleted_cwd_orphans)
                        if ! _dispatch_sweep_remaining; then
                            _sweep_budget_exhausted=1
                        fi
                    else
                        _sweep_budget_exhausted=1
                        _orphans_killed=0
                    fi
                else
                    _orphans_killed=0
                fi
            case "\$_orphans_killed" in ''|*[!0-9]*) _orphans_killed=0 ;; esac
            # Orphan lane-venv reaper [RES-07][OBS-08]: persisted venvs live
            # outside the sandbox so the per-pass wipe leaves them behind. When
            # the sandbox is already gone the marker-gated sweep never sees the
            # pair; reclaim venv+stamp only when all six hold: directory, not
            # this dispatch's key, no sandbox path, lease not live, lock not
            # held, older than SANDBOX_TTL_SEC. Half-pairs are their own leak.
            for _vd in "\$ROOT"/.venv-lane-*/; do
                if ! _dispatch_sweep_remaining; then
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
                # Live-lane fail-safe: lease is written BEFORE per-pass rm -rf "\$SBX".
                if _lane_occupant_live "\$_sk"; then
                    continue
                fi
                # Held lane lock: same wipe/re-extract window, stronger signal.
                if _lane_lock_held "\$_sk"; then
                    continue
                fi
                _reap_mtime "\$_vd" || continue
                [ "\$((_now - _mt))" -gt ${SANDBOX_TTL_SEC} ] || continue
                _age=\$((_now - _mt))
                _before=\$(du -sb "\$_vd" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null | awk '{sum += \$1} END {print sum + 0}' || true)
                case "\$_before" in ''|*[!0-9]*) _before=0 ;; esac
                if ! printf '{"key":"%s","outcome":"started","reason":"orphan_venv","age":%s,"actor":"dispatch_ttl","bytes_before":%s,"at":%s}\n' \
                    "\$_sk" "\$_age" "\$_before" "\$(date +%s)" >>"\$ROOT/.reap-outcomes.jsonl"; then
                    echo "remote_agent: cannot persist orphan venv reap intent for \$_sk; deletion refused" >&2
                    continue
                fi
                _del_rc=0
                rm -rf "\$_vd" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null || _del_rc=\$?
                _probe_rc=0
                [ -e "\$_vd" ] || _probe_rc=\$?
                if _journal_deletion_outcome "\$_sk" "\$_del_rc" "\$_probe_rc" orphan_venv "\$_age" "\$_before"; then
                    _reclaimed_orphan_venvs=\$((_reclaimed_orphan_venvs + 1))
                    _sweep_reaped=\$(( _sweep_reaped + 1 ))
                    _sweep_freed_kb=\$(( _sweep_freed_kb + _before / 1024 ))
                fi
            done
            if [ "\$_sweep_budget_exhausted" -eq 1 ]; then
                echo 'remote_agent: sandbox sweep budget exhausted; remaining candidates deferred' >&2
            else
                rm -f "\$_sweep_cursor_file" "\$_sweep_cursor_file.tmp" 2>/dev/null || true
                _sweep_done_at=\$(date +%s)
                if [ "\$_sweep_pressured" -eq 1 ]; then
                    _sweep_pressured_json=true
                else
                    _sweep_pressured_json=false
                fi
                if ! printf '{"at":%s,"pressured":%s,"candidates":%s,"reaped":%s,"freed_kb":%s,"actor":"dispatch_ttl"}\n' \
                    "\$_sweep_done_at" "\$_sweep_pressured_json" "\$_sweep_candidates" "\$_sweep_reaped" "\$_sweep_freed_kb" \
                    >"\$_sweep_stamp.tmp" 2>/dev/null || ! mv -f "\$_sweep_stamp.tmp" "\$_sweep_stamp" 2>/dev/null; then
                    echo 'remote_agent: cannot persist dispatch sandbox sweep stamp — sweep remains fail-open' >&2
                    rm -f "\$_sweep_stamp.tmp" 2>/dev/null || true
                fi
            fi
            fi
            # One stderr line per non-empty pass; never stdout (dispatch protocol).
            if [ "\$_reclaimed_sandboxes" -gt 0 ] || [ "\$_reclaimed_orphan_venvs" -gt 0 ]; then
                printf 'remote_agent: reaper reclaimed sandboxes=%s orphan_venvs=%s\n' \
                    "\$_reclaimed_sandboxes" "\$_reclaimed_orphan_venvs" >&2
            fi
        fi
    } || true
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
    for _evidence_file in .grok-result.json .grok-phases.json .workbay-source-provenance.json .agent-stream.jsonl .grok-debug.log .grok-selfverify.json uncommitted-remainder.patch; do
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
        .brief.md|.schema.json|.grok-result.json|.grok-run.log|.grok-debug.log|.grok-selfverify.json|.grok-selfverify.log|.venv|.venv/*|.workbay-lane-sandbox|.workbay-source-provenance|.agent-stream.jsonl)
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
printf '%s\n' .brief.md .schema.json .grok-result.json .grok-run.log .grok-debug.log .grok-selfverify.json .grok-selfverify.log .venv .workbay-lane-sandbox .workbay-source-provenance .agent-stream.jsonl .review/ > .git/info/exclude
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
    if "\$UV_BIN" sync --offline -q >&2 </dev/null 9>&-; then
        return 0
    fi
    echo 'remote_agent: offline uv cache incomplete — trying bounded network sync' >&2
    if command -v timeout >/dev/null 2>&1 && \
        UV_HTTP_TIMEOUT=10 UV_HTTP_RETRIES=1 timeout -k 5 "\${UV_SYNC_NETWORK_TIMEOUT_SEC}" \
            "\$UV_BIN" sync -q >&2 </dev/null 9>&-; then
        return 0
    fi
    echo 'remote_agent: uv sync failed (uv_sync_failed): offline cache incomplete and bounded network sync failed' >&2
    return 1
}
# END UV_SYNC_CACHE_FALLBACK
# Lockfile-hash sync gate (internal S2): skip 'uv sync'
# entirely when uv.lock + every pyproject.toml are byte-identical to the last
# successful sync for this lane AND the persisted venv still exists. The stamp
# lives outside \$SBX (survives the per-pass wipe), keyed by LANE_KEY. Fail-open:
# if sha256sum is unavailable the hash is empty and we always sync (today's
# behavior); any dependency edit changes the hash and forces a re-sync.
SYNC_STAMP="\$ROOT/.venv-sync-stamp-${LANE_KEY}"
_dep_hash=""
_sync_start=\$(date +%s)
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
        _uv_sync_with_cache_fallback || { echo 'remote_agent: uv sync failed (uv_sync_failed)' >&2; exit 1; }
    fi
    # Stamp the dep hash only AFTER a successful sync so an aborted/failed sync
    # never records a warm-skip for a half-populated venv (fail-open to re-sync).
    [ -n "\$_dep_hash" ] && printf '%s\n' "\$_dep_hash" > "\$SYNC_STAMP" 2>/dev/null || true
fi
# VM-LANE-VENV-EDITABLE-SYNC-SHIPS-SCRUBBED-COPY-SHADOWS-WORKTREE-SOURCES-01:
# Probe the actual install in isolation: inherited PYTHONPATH must not hide a
# frozen site-packages copy. Emit evidence BEFORE removing the derived venv.
_ORIGIN_GUARD_STATUS=''
_origin_guard_refuse() {
    local reason="\$1"
    echo "remote_agent: venv_origin_shadow \$reason" >&2
    _ORIGIN_GUARD_STATUS=refused
    _PHASES_JSON_PARTS="\${_PHASES_JSON_PARTS:+\${_PHASES_JSON_PARTS},}\"origin_guard\":{\"origin_guard\":\"refused\",\"reason\":\"\$reason\"}"
}
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
    "\$HOME/.local/bin/uv" sync -q >&2 </dev/null 9>&- || { echo 'remote_agent: uv sync failed during origin repair' >&2; exit 1; }
    _assert_lane_venv_origin || { echo 'remote_agent: uv sync failed to repair lane import origin' >&2; exit 1; }
    [ -n "\$_dep_hash" ] && printf '%s\n' "\$_dep_hash" > "\$SYNC_STAMP" 2>/dev/null || true
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
_phase_record sync "\$_sync_start" "\$_sync_end"
# Add the attestation to the sync phase just recorded (remove its closing }).
case "\$_ORIGIN_GUARD_STATUS" in
    verified_roots) _origin_ok=1 ;;
    skipped_no_guarded_roots) _origin_ok=null ;;
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
    _uv_sync_with_cache_fallback || {
        echo 'remote_agent: lane venv rebuild failed' >&2
        return 1
    }
    ln -sfn "\$LANE_VENV" "\$SBX/.venv" 2>/dev/null || true
    if [ ! -d "\$SBX/.venv/bin" ]; then
        echo 'remote_agent: \$SBX/.venv still unusable after rebuild — PATH contract would lie' >&2
        return 1
    fi
    return 0
}
# Message must contain 'uv sync failed' so exit-1 producer→adapter mapping
# classifies as uv_sync_failed (sibling owns remote_exec.py; do not add a new
# unclassified exit-1 phrase here).
_ensure_lane_venv_symlink || { echo 'remote_agent: uv sync failed — lane venv unusable (dangling .venv)' >&2; exit 1; }
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
    _uv_cache_dir="\${UV_CACHE_DIR:-\$HOME/.cache/uv}"
    _uv_measure_kb() {
        _uv_measure_raw=
        if ! _uv_measure_raw="\$(du -sk "\$1" 2>/dev/null)"; then
            return 1
        fi
        _uv_measure_value="\$(printf '%s\\n' "\$_uv_measure_raw" | awk 'NF >= 1 && \$1 ~ /^[0-9]+$/ { print \$1; found=1 } END { if (!found) exit 1 }')" || return 1
        case "\$_uv_measure_value" in ''|*[!0-9]*) return 1 ;; esac
        printf '%s' "\$_uv_measure_value"
    }
    if ! _uv_before_kb="\$(_uv_measure_kb "\$_uv_cache_dir")"; then
        echo 'remote_agent: uv_cache_unmeasurable: pre-prune du failed or returned a non-numeric size' >&2
        return 75
    fi
    _uv_before_mb=\$(( (_uv_before_kb + 1023) / 1024 ))
    _uv_after_mb="\$_uv_before_mb"
    if [ "\$_uv_before_kb" -gt \$((UV_CACHE_CAP_MB * 1024)) ]; then
        type timeout >/dev/null 2>&1 || {
            echo 'remote_agent: timeout unavailable for uv cache prune' >&2
            return 75
        }
        if ! UV_CACHE_DIR="\$_uv_cache_dir" timeout 90 "\$HOME/.local/bin/uv" cache prune >&2 </dev/null 9>&-; then
            echo 'remote_agent: bounded uv cache prune failed' >&2
            return 75
        fi
        if ! _uv_after_kb="\$(_uv_measure_kb "\$_uv_cache_dir")"; then
            echo 'remote_agent: uv_cache_unmeasurable: post-prune du failed or returned a non-numeric size' >&2
            return 75
        fi
        _uv_after_mb=\$(( (_uv_after_kb + 1023) / 1024 ))
    fi
    echo "remote_agent: uv_cache_mb_before=\$_uv_before_mb uv_cache_mb_after=\$_uv_after_mb" >&2
    return 0
}
# --- workbay: uv-cache-bound-function (end) ---
_bound_uv_cache || { _uv_cache_rc=\$?; exit "\$_uv_cache_rc"; }
# An OOM-killed prior run leaves ${LANE_UNIT} in systemd 'failed' state,
# which refuses the unit name on the next run of the same lane — clear it
# first (no-op when absent). LANE_UNIT already includes .scope (same name
# systemd-run --scope registers). LANE_UNIT_SV carries its own .scope suffix
# for the same reason: a killed RuntimeMaxSec-only self-verify leaves that
# name failed and the next same-lane dispatch is refused the unit (rc charged
# to TEST_CMD) [RES-01].
systemctl --user reset-failed ${LANE_UNIT} 2>/dev/null || true
systemctl --user reset-failed ${LANE_UNIT_SV} 2>/dev/null || true
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
# Single assembly site: whole RUNNER string from the bound ladder [TEST-15].
# RuntimeMaxSec uses the residual already checked above (not a second clock).
RUNNER="\$(_agent_bound_runner "\$_grok_budget")"
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
    # consumer). Never leave _sv_tw empty while _sv_cmd is set: self-verify
    # runs after the grok scope returns, so RuntimeMaxSec on that unit does
    # not cover it; an unbound TEST_CMD holds the lane flock forever [RES-02].
    _sv_tw=''
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
    elif command -v timeout >/dev/null 2>&1; then
        _sv_tw="timeout -k 5 \$_sv_budget"
    elif [ "\$_SCOPE_SUPPORTS_RUNTIMEMAX" = 1 ]; then
        _sv_tw="systemd-run --quiet --user --scope --unit ${LANE_UNIT_SV} -p RuntimeMaxSec=\$_sv_budget"
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
        if ( cd "\$SBX" && PATH="\$HOME/.local/bin:\$SBX/.venv/bin:\$PATH" VIRTUAL_ENV="\$SBX/.venv" WORKBAY_ORIGIN_GUARD_NONCE="\$_sv_guard_nonce" \$_sv_tw python3 -c '
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
' "\$_sv_log" "\$_sv_cmd" </dev/null 9>&- ); then _sv_rc=0; else _sv_rc=\$?; fi
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
if log_nonregular:
    outcome = "harness_error"
    reason = "selfverify_log_nonregular"
elif budget_expired:
    outcome = "failed"
    reason = "self-verify processing budget exhausted"
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
export AGENT_SPEC_RESULT_FILE="\$OUT_DIR/.grok-result.json"
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
    # Launch: optional env_file sourced only inside the agent subshell (D6 —
    # credentials must not leak into the D9 post-classifier). exec is the sole
    # AGENT_SPEC_BIN launch site. Capture rc from the else branch of a
    # non-negated if: under POSIX an if whose condition is a negated command
    # always leaves rc at 0 (HARM-H01).
    # Tilde in env_file is expanded here (~ inside host single-quotes does not).
    _envf='${AGENT_SPEC_ENV_FILE}'
    case "\$_envf" in
        '~/'*) _envf="\$HOME/\${_envf#\~/}" ;;
    esac
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
        if [ -n "\$_envf" ] && [ -f "\$_envf" ]; then
            set -a
            # shellcheck disable=SC1090
            . "\$_envf"
            set +a
        fi
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
        exec \$RUNNER \$TW python3 "\$_model_start_boundary" \
                "\$OUT_DIR/.workbay-source-provenance.json" \
                '${AGENT_SPEC_BIN}' "\${AGENT_ARGV[@]}" \
                >"\$_spec_stdout" 2>"\$_spec_stderr" <"\$_spec_stdin" 9>&-
    ); then
        _agent_rc=0
    else
        _agent_rc=\$?
    fi
    _AGENT_END_TS=\$(date +%s)
    _phase_record agent_turn "\$_AGENT_START_TS" "\$_AGENT_END_TS"
    # Post-classify from AgentSpec auth_match + result shape (implementation note D7/D9).
    # Patterns/streams live in the JSON spec — never as vendor literals here.
    _spec_json="\$ROOT/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json"
    _post=\$(python3 - "\$_spec_json" "\$_spec_stdout" "\$_spec_stderr" "\$_agent_rc" <<'PY'
import json
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
    return "\n".join(evidence)

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
            stream_text.append(text)
        except OSError as exc:
            print(f"remote_agent: classifier stream unreadable ({name}): {exc}", file=sys.stderr)
    return "\n".join(stream_text)

blob = _stream_blob(auth, auth_only=True)
rate_blob = _stream_blob(rate_limit, rate_only=True)
# auth_only: stderr raw + stdout error records only. Worker narration that
# merely quotes a tier string is data, not a provider tier warning.
service_tier_warning_blob = _stream_blob(service_tier_warning, auth_only=True)
# precedence: exit_codes_then_patterns (implementation note D6)
if agent_rc in exit_codes:
    print("auth_failed")
    raise SystemExit(0)
if agent_rc != 0 and (agent_rc in rate_exit_codes or (rate_patterns and any(p in rate_blob for p in rate_patterns))):
    import re

    matching_line = next(
        (line for line in rate_blob.splitlines() if any(pattern in line for pattern in rate_patterns)),
        f"exit code {agent_rc}",
    )
    print(f"remote_agent: rate_limited (upstream quota): {matching_line[:200]}", file=sys.stderr)
    reset_match = re.search(
        r'(?i)(?:x-ratelimit-reset|retry-after|reset)["\s]*[:=]["\s]*([^",}\s]+)',
        rate_blob,
    )
    if reset_match:
        print(f"remote_agent: rate_limit_reset={reset_match.group(1)}", file=sys.stderr)
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
    # authority (structuredOutput before narrated channels; dict fields too).
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
    #   2) structuredOutput (beats narrated channels — REVC-003)
    #   3) dict-valued envelope fields (HARM-03; host _extract_review_payload)
    #   4) string channels, first channel with a shaped object, last-wins inside
    # Recovered inner payloads are PROMOTED onto the artifact (DURREV-VM-F1).
    recovered = None
    if isinstance(payload, dict):
        if _is_shaped(payload):
            recovered = payload
        else:
            so_hit = _structured_shaped(payload.get("structuredOutput"))
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
        _AGENT_SPEC_RESOLVER_SRC="$(declare -f _agent_spec_resolve_argv)"
        _SUBMIT_JOB_SRC="$(declare -f _remote_submit_job)"
        _body_b64="$(_emit_remote_body | base64 | tr -d '\n')"
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
BODY_B64='${_body_b64}'
export BRIEF_PATH="\$HOME/${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md"
export OUTBOX_PATH="\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}"
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

    fetch_result() {
        [ -n "$RESULT_OUT" ] || return 0
        # Best-effort: grok's stdout JSON lives in this dispatch's outbox (outside
        # \$SBX), so fetch it even on a no-change / grok-fail exit — the caller can
        # still surface grok's summary/blockers. Missing file is non-fatal.
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-result.json" "$RESULT_OUT" 2>/dev/null; then
            echo "remote_agent: result written -> $RESULT_OUT" >&2
        else
            echo "remote_agent: no result json fetched (grok emitted no stdout?)" >&2
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
        _phases_fetch_tmp="${PHASES_OUT}.fetch.$$"
        if _scp_with_deadline \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-phases.json" \
                "$_phases_fetch_tmp" 2>/dev/null \
            && [ -s "$_phases_fetch_tmp" ] \
            && mv -f "$_phases_fetch_tmp" "$PHASES_OUT"; then
            echo "remote_agent: phases written -> $PHASES_OUT" >&2
            return 0
        else
            rm -f "$_phases_fetch_tmp" 2>/dev/null || true
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
    if [ "$cmd" = submit ]; then
        _phase "submitting detached remote grok job ${JOB_ID} (residual ${GROK_TIMEOUT}s)"
        if run_submit; then rc=0; else rc=$?; fi
        exit "$rc"
    fi
    _phase "materializing sandbox + dispatching remote grok build (residual ${GROK_TIMEOUT}s)"
    if [ -n "$OUT" ]; then
        if run_patch > "$OUT"; then rc=0; else rc=$?; fi
        if [ "$rc" -eq 0 ]; then
            echo "remote_agent: patch written -> $OUT ($(grep -c '^Subject:' "$OUT") commit(s))" >&2
        fi
    else
        if run_patch; then rc=0; else rc=$?; fi
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
    exit "$rc"
    # END BUILD_DISPATCH
    ;;
*)
    sed -n '15,26p' "$0" >&2
    exit 2
    ;;
esac
