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
#       [--model grok-4.5] [--max-turns 40] [--effort high] [--out <patch>] \
#       [--result-out <json>] [--debug-out <log>] [--timeout <seconds>]
#   scripts/remote_agent.sh doctor          # grok readiness on the host
#
#   --out FILE         write grok's committed changes as a git patch to FILE.
#   --result-out FILE  write grok's structured stdout JSON (the BackendResult
#                      envelope) to FILE, best-effort: fetched even on a no-change /
#                      grok-fail exit so the caller can surface grok's summary/blockers.
#   --debug-out FILE   write grok's --debug-file log to FILE, best-effort: lets the
#                      caller run the post-turn grok-build contamination backstop
#                      (decision #2799 pin guard) on the same evidence GrokCliAdapter uses.
#   --test-cmd CMD     off-box self-verify (item 26): after grok commits, run CMD in the
#                      sandbox venv, capturing {command,exit_code,passed,output_tail} JSON.
#                      The patch is still emitted regardless of the result — the caller
#                      (worker) gates on the captured outcome; this script only measures.
#   --selfverify-out FILE  fetch the off-box self-verify JSON to FILE, best-effort (only
#                      written when --test-cmd was supplied and grok committed).
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
# Config precedence: process env always wins over `.workbay/remote-gate.env` for
# every WORKBAY_* knob (env is snapshotted before the file is sourced). The config
# file may only supply HOST/DIR fallback keys (`REMOTE_GATE_HOST`, `REMOTE_GATE_DIR`);
# caps / floor / lanes / sandbox-root are env-only (script defaults when unset).
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
#       · 78 host not configured · 2 usage/validation error.
#
# Concurrency: lanes on DISTINCT branches run concurrently up to
# WORKBAY_REMOTE_AGENT_MAX_LANES. Lanes on the SAME branch share a LANE_KEY (it is
# derived from the branch name), hence one sandbox path — they are serialized by a
# non-blocking lane lock and the loser defers with exit 75. It is never correct for
# one lane to wipe another's live sandbox (internal).
set -euo pipefail

repo_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
repo_slug="$(basename "$repo_root")"

# Snapshot every WORKBAY_* knob BEFORE sourcing the config file so process env
# always wins (a file that exports WORKBAY_REMOTE_GATE_MEM_FLOOR_MB=0 must not
# silently zero the floor when the operator set a valid value in the env).
# File keys that remain in scope after source are only HOST/DIR fallbacks
# (REMOTE_GATE_HOST / REMOTE_GATE_DIR) — mirror of remote_gate.sh _env_* pattern.
_env_host="${WORKBAY_REMOTE_GATE_HOST:-}"
_env_dir="${WORKBAY_REMOTE_GATE_DIR:-}"
_env_agent_root="${WORKBAY_REMOTE_AGENT_ROOT:-}"
_env_mem_max="${WORKBAY_REMOTE_GATE_MEMORY_MAX:-}"
_env_cpu_quota="${WORKBAY_REMOTE_GATE_CPU_QUOTA:-}"
_env_mem_floor="${WORKBAY_REMOTE_GATE_MEM_FLOOR_MB:-}"
_env_max_lanes="${WORKBAY_REMOTE_AGENT_MAX_LANES:-}"
_env_max_lane_venvs="${WORKBAY_REMOTE_AGENT_MAX_LANE_VENVS:-}"
_env_dispatch_ttl="${WORKBAY_REMOTE_AGENT_DISPATCH_TTL_SEC:-}"
_env_sandbox_ttl="${WORKBAY_REMOTE_AGENT_SANDBOX_TTL_SEC:-}"
_env_keep_refs="${WORKBAY_REMOTE_AGENT_KEEP_REFS:-}"
_env_reap_legacy="${WORKBAY_REMOTE_AGENT_REAP_LEGACY_REFS:-}"
_env_unbounded_ceiling="${WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S:-}"
REMOTE_GATE_HOST="" REMOTE_GATE_DIR=""
config_file="$repo_root/.workbay/remote-gate.env"
# shellcheck disable=SC1090
[ -f "$config_file" ] && . "$config_file"

REMOTE_HOST="${_env_host:-${REMOTE_GATE_HOST:-}}"
if [ -z "$REMOTE_HOST" ]; then
    echo "remote_agent: host not configured — set WORKBAY_REMOTE_GATE_HOST or" \
         "REMOTE_GATE_HOST in .workbay/remote-gate.env (e.g. gate@<your-host>)" >&2
    exit 78
fi
REMOTE_DIR="${_env_dir:-${REMOTE_GATE_DIR:-src/${repo_slug}}}"
# Caps / floor / lanes / sandbox-root: env snapshot only (defaults when unset).
# Never re-read WORKBAY_* after source — file must not override env ([REF-10]).
AGENT_ROOT="${_env_agent_root:-grok-sandbox}"
MEM_MAX="${_env_mem_max:-6G}"
CPU_QUOTA="${_env_cpu_quota:-200%}"
# Arm-3 ceiling for --timeout 0 (seconds): injectable so V3/V6 can shrink it;
# default 21600 matches the pre-ladder 6h lease ceiling [RES-02][FM-08].
# Read from the pre-source env snapshot only — never re-read WORKBAY_* after
# sourcing remote-gate.env ([REF-10] process env wins over file).
WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S="${_env_unbounded_ceiling:-21600}"
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
DISPATCH_TTL_SEC="${_env_dispatch_ttl:-86400}"
# Age TTL (seconds) for PER-LANE sandboxes ($ROOT/<LANE_KEY>) [RES-07]: these leak
# forever when a lane key is never re-dispatched (same-key wipe is the only prior
# reclaim path). Default 172800 (48h) so a post-mortem sandbox survives a
# weekend-adjacent gap; must stay >= 24h so a paused/deferred lane is not reaped
# while still wanted. 0 = disable only this sweep (dispatch reaper independent).
SANDBOX_TTL_SEC="${_env_sandbox_ttl:-172800}"
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
case "$LANE_VENV_CAP" in *[!0-9]*|"") echo "remote_agent: MAX_LANE_VENVS must be a non-negative integer (0=keep all)" >&2; exit 2 ;; esac
case "$DISPATCH_TTL_SEC" in *[!0-9]*|"") echo "remote_agent: DISPATCH_TTL_SEC must be a non-negative integer (0=disable)" >&2; exit 2 ;; esac
case "$SANDBOX_TTL_SEC" in *[!0-9]*|"") echo "remote_agent: SANDBOX_TTL_SEC must be a non-negative integer (0=disable)" >&2; exit 2 ;; esac
case "$WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S" in *[!0-9]*|"") echo "remote_agent: WORKBAY_REMOTE_AGENT_UNBOUNDED_CEILING_S must be an integer >= 1 (0 cannot bound anything; refuse at admission, not exit 75)" >&2; exit 2 ;; esac
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

cmd="${1:-}"; [ "$#" -gt 0 ] && shift

case "$cmd" in
doctor)
    "${SSH[@]}" 'g="$HOME/.grok/bin/grok"
        [ -x "$g" ] && echo "grok    : $("$g" --version)" || echo "grok    : MISSING (install per runbook)"
        [ -f "$HOME/.grok/auth.json" ] && echo "auth    : present (perms $(stat -c %a "$HOME/.grok/auth.json"))" || echo "auth    : MISSING (run: grok login --device-auth)"
        [ -x "$HOME/.local/bin/uv" ] && echo "uv      : $("$HOME/.local/bin/uv" --version)" || echo "uv      : MISSING"
        command -v systemd-run >/dev/null && echo "caps    : systemd-run available" || echo "caps    : systemd-run MISSING (nice/ionice only)"
        root="$HOME/'"$AGENT_ROOT"'"
        n=$(ls -1d "$root"/.venv-lane-* 2>/dev/null | wc -l | tr -d " ")
        tot=$(du -csh "$root"/.venv-lane-* 2>/dev/null | tail -1 | cut -f1)
        echo "venvs   : ${n:-0} persisted lane venv(s)${tot:+, ~$tot total} (cap '"$LANE_VENV_CAP"')"'
    ;;
build)
    # implementation note / decision 4134 (RES-13 crumple zone): close inherited stdin.
    # The orchestrator spawns this script with the MCP server's own stdin — the
    # JSON-RPC stdio pipe, a non-tty, never-EOF fd. Without this, the step-1
    # `git push` below (git's default ssh) blocks reading it forever, burning the
    # whole timeout with no VM sandbox and 0 grok output. All real input arrives
    # via --brief/--schema files and ssh heredocs (which set their own stdin), so
    # /dev/null is safe and only removes the block — robust regardless of caller.
    exec </dev/null
    BRANCH="" BRIEF="" SCHEMA="" MODEL="grok-4.5" MAX_TURNS="40" EFFORT="high" OUT="" RESULT_OUT="" DEBUG_OUT="" TIMEOUT="0" SELFVERIFY_CMD="" SELFVERIFY_OUT=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --branch) BRANCH="${2:-}"; shift 2 ;;
            --brief) BRIEF="${2:-}"; shift 2 ;;
            --schema) SCHEMA="${2:-}"; shift 2 ;;
            --model) MODEL="${2:-}"; shift 2 ;;
            --max-turns) MAX_TURNS="${2:-}"; shift 2 ;;
            --effort) EFFORT="${2:-}"; shift 2 ;;
            --out) OUT="${2:-}"; shift 2 ;;
            --result-out) RESULT_OUT="${2:-}"; shift 2 ;;
            --debug-out) DEBUG_OUT="${2:-}"; shift 2 ;;
            --timeout) TIMEOUT="${2:-}"; shift 2 ;;
            --test-cmd) SELFVERIFY_CMD="${2:-}"; shift 2 ;;
            --selfverify-out) SELFVERIFY_OUT="${2:-}"; shift 2 ;;
            *) die "unknown arg: $1" ;;
        esac
    done
    [ -n "$BRANCH" ] || die "--branch required"
    [ -f "$BRIEF" ] || die "--brief file not found: ${BRIEF:-<unset>}"
    [ -f "$SCHEMA" ] || die "--schema file not found: ${SCHEMA:-<unset>}"
    case "$BRANCH" in *[!A-Za-z0-9/_.-]*) die "unsafe --branch name" ;; esac
    case "$MAX_TURNS" in *[!0-9]*|"") die "--max-turns must be an integer" ;; esac
    case "$MODEL" in *[!A-Za-z0-9._-]*) die "unsafe --model" ;; esac
    case "$EFFORT" in low|medium|high|xhigh) : ;; *) die "--effort must be low|medium|high|xhigh" ;; esac
    case "$TIMEOUT" in *[!0-9]*|"") die "--timeout must be a non-negative integer (seconds; 0=none)" ;; esac
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
    DISPATCH_NONCE="${$}-$(od -An -N8 -tx1 /dev/urandom 2>/dev/null | tr -d ' \n' || true)"
    # Fallback must keep the ONE system shape <pid>-<16hex> so the ref reaper
    # name guard can match both mint paths [REF-10][RES-07].
    [ -n "${DISPATCH_NONCE#*-}" ] || DISPATCH_NONCE="${$}-$(printf '%08x%08x' "$(date +%s)" "${RANDOM:-0}")"
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
        # dispatch's nonce AND is unexpired; unparseable expiry fails open as
        # live), spare every transient for the age-based reaper [RES-07] — the
        # remote scope is designed to outlive the local dispatcher, and
        # unlinking its outbox converts a transport kill into total loss of
        # completed work. Lease check and removals share one ssh round-trip
        # so there is no test-then-delete race.
        if ! "${SSH[@]}" "_lv=\"\$HOME/${AGENT_ROOT}/.lane-live-${LANE_KEY}\"; \
            _live=0; \
            if [ -f \"\$_lv\" ] && grep -qx 'nonce=${DISPATCH_NONCE}' \"\$_lv\" 2>/dev/null; then \
                _exp=\$(sed -n 's/^expiry=//p' \"\$_lv\" 2>/dev/null | head -n1); \
                case \"\$_exp\" in \
                    ''|*[!0-9]*) _live=1 ;; \
                    *) if [ \"\$_exp\" -gt \"\$(date +%s)\" ]; then _live=1; fi ;; \
                esac; \
            fi; \
            if [ \"\$_live\" = 1 ]; then \
                echo 'remote_agent: live lane holds this dispatch nonce — sparing outbox for age-based reaper' >&2; \
            else \
                rm -f \
                \"\$HOME/${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md\" \
                \"\$HOME/${AGENT_ROOT}/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-result.json\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-debug.log\" \
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-selfverify.json\" \
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
ADMISSION_EOF
    _admission_remote_sh="${_admission_tpl//__MEM_FLOOR_MB__/${MEM_FLOOR_MB}}"
    _admission_remote_sh="${_admission_remote_sh//__MAX_LANES__/${MAX_LANES}}"

    # 0) PRE-dispatch admission probe BEFORE any transfer cost. Exit 75 is the
    # same retryable-defer contract as the in-run check (TOCTOU belt-and-
    # suspenders — keep both).
    # shellcheck disable=SC2029
    "${SSH[@]}" bash -s <<REMOTE_EOF >&2
set -euo pipefail
${_admission_remote_sh}
REMOTE_EOF

    # 1) push committed HEAD of the branch to the remote clone (only committed state is built).
    # Push target is LANE_KEY + per-dispatch nonce: LANE_KEY alone is shared by
    # concurrent same-branch dispatches and would let the second overwrite the
    # first's ref mid-run. The full $BRANCH source ref is preserved as the local
    # side of the refspec.
    _phase "pushing $BRANCH -> ${REMOTE_HOST}:${REMOTE_DIR} (refs/heads/${LANE_KEY}-${DISPATCH_NONCE})"
    # BatchMode/ConnectTimeout (matching the SSH array): the push must FAIL FAST,
    # never prompt or hang, even if a caller leaves stdin attached — belt to the
    # `exec </dev/null` above (implementation note).
    GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=4' \
        git push --quiet --force "${REMOTE_HOST}:${REMOTE_DIR}" "${BRANCH}:refs/heads/${LANE_KEY}-${DISPATCH_NONCE}" >&2
    # Arm EXIT cleanup at the FIRST remote state (the push). Waiting until after
    # both scps leaves the pushed ref (and possibly the first copy) stranded on
    # a mid-staging failure [RES-13].
    _dispatch_staged=1

    # 2) ship brief + schema to the sandbox PARENT (survives the sandbox wipe).
    # Paths carry DISPATCH_NONCE so a concurrent same-branch dispatch cannot
    # overwrite this lane's inputs before/while the lock is held [CON-11].
    "${SSH[@]}" "mkdir -p \"\$HOME/${AGENT_ROOT}\"" >&2
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$BRIEF"  "${REMOTE_HOST}:${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md"   >&2
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$SCHEMA" "${REMOTE_HOST}:${AGENT_ROOT}/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json" >&2

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
    run_patch() {
        # shellcheck disable=SC2029
        "${SSH[@]}" bash -s <<REMOTE_EOF
set -euo pipefail
export PATH="\$HOME/.grok/bin:\$PATH"
export GROK_ZDR_ENABLED=1
# VM admission (RES-14 backpressure): re-check floor + lane cap at run start
# (TOCTOU vs pre-dispatch probe). Exit 75 → adapter maps to admission_deferred.
${_admission_remote_sh}
# RES-02 (S3): mark in-sandbox start so the setup + uv-sync time below can be
# subtracted from grok's residual budget. The pre-dispatch GROK_TIMEOUT math ran
# locally BEFORE this remote body, so it does not yet account for sandbox setup.
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
# Bounded disk growth (internal S4): persisted lane venvs
# accumulate one per distinct branch. Keep the ${LANE_VENV_CAP} most-recently-used
# (by mtime); LRU-evict older ones with their sync stamps. Never evict THIS lane's
# venv. Best-effort / fail-open — a reap error must never wedge the pass. 0=keep all.
if [ '${LANE_VENV_CAP}' -gt 0 ] 2>/dev/null; then
    # '|| true' on the whole pipeline: under 'set -euo pipefail' a non-matching
    # .venv-lane-* glob makes 'ls' exit non-zero (no nullglob), pipefail
    # propagates it, and set -e would abort the pass BEFORE materialize — a
    # cold-start deadlock on any lane with zero persisted venvs (fresh VM /
    # post-cleanup). Fail-open for real: a reap error must never wedge the pass.
    ls -1dt "\$ROOT"/.venv-lane-* 2>/dev/null | tail -n +\$(( ${LANE_VENV_CAP} + 1 )) | while IFS= read -r _old; do
        [ "\$_old" = "\$LANE_VENV" ] && continue
        _oldkey="\${_old##*/.venv-lane-}"
        rm -rf "\$_old" "\$ROOT/.venv-sync-stamp-\${_oldkey}" 2>/dev/null || true
    done || true
fi
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
    rm -f "\$_lane_lease_file" 2>/dev/null || true
}
_lane_occupant_live() {
    _lk="\${1:-}"
    # Empty key: fail-safe OCCUPIED (caller/reaper also short-circuits unparseable).
    [ -n "\$_lk" ] || return 0
    _lf="\$ROOT/.lane-live-\${_lk}"
    [ -f "\$_lf" ] || return 1
    _expiry=
    _issued=
    while IFS= read -r _lline || [ -n "\$_lline" ]; do
        # Substring (not \${var#pfx}): extractors strip # as comments [TEST-04].
        case "\$_lline" in
            expiry=*) _expiry="\${_lline:7}" ;;
            issued=*) _issued="\${_lline:7}" ;;
        esac
    done < "\$_lf" || return 0
    case "\$_expiry" in
        ''|*[!0-9]*) return 0 ;;
    esac
    case "\$_issued" in
        ''|*[!0-9]*) return 0 ;;
    esac
    _now=\$(date +%s)
    # Expired → clear (not occupied).
    if [ "\$_now" -ge "\$_expiry" ]; then
        return 1
    fi
    # Unexpired (including now < issued after a backward jump) → OCCUPIED.
    return 0
}
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
    if ! printf 'pid=%s\nissued=%s\nexpiry=%s\nnonce=%s\n' \
        "\$\$" "\$_now" "\$_expiry" '${DISPATCH_NONCE}' > "\${_lane_lease_file}.tmp" \
        || ! mv -f "\${_lane_lease_file}.tmp" "\$_lane_lease_file"; then
        echo 'remote_agent: failed to write occupancy lease — refusing to wipe sandbox' >&2
        return 1
    fi
    trap '_lane_clear_live_lease' EXIT
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
            _mt=\$(stat -c %Y "\$1" 2>/dev/null) || _mt=\$(stat -f %m "\$1" 2>/dev/null) || {
                if [ -z "\${_reap_mtime_warned:-}" ]; then
                    echo 'remote_agent: dispatch reaper cannot probe mtime (need GNU stat -c %Y or BSD stat -f %m) — sweep degraded' >&2
                    _reap_mtime_warned=1
                fi
                return 1
            }
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
        _now=\$(date +%s)
        # Bake own nonce at local dispatch (unquoted heredoc expands it into the
        # single-quoted literal). Remote never needs DISPATCH_NONCE set under -u.
        # Extract-only harnesses that leave the token unsubstituted get a no-op
        # self-exclusion (literal '\${DISPATCH_NONCE}' matches no real path).
        _self_nonce='${DISPATCH_NONCE}'
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
            rm -rf "\$_p" 2>/dev/null || true
        done
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
            git -C "\$SRC" update-ref -d "refs/heads/\$_bn" 2>/dev/null || true
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
            for _sd in "\$ROOT"/*/; do
                _sd="\${_sd%/}"
                # Marker-gated: unmarked operator dirs are never candidates.
                if [ ! -f "\$_sd/.workbay-lane-sandbox" ]; then
                    continue
                fi
                # Never reap this dispatch's own sandbox (or its warm venv).
                if [ "\$_sd" = "\$SBX" ]; then
                    continue
                fi
                _sk="\${_sd##*/}"
                # Live-lane fail-safe: unexpired/malformed lease outranks age.
                if _lane_occupant_live "\$_sk"; then
                    continue
                fi
                # Held lane lock outranks age (stronger than lease; no clock).
                if _lane_lock_held "\$_sk"; then
                    continue
                fi
                # Age from the marker mtime (stable while work happens in subdirs).
                _reap_mtime "\$_sd/.workbay-lane-sandbox" || continue
                [ "\$((_now - _mt))" -gt ${SANDBOX_TTL_SEC} ] || continue
                rm -rf "\$_sd" "\$ROOT/.venv-lane-\$_sk" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null || true
                # Count only real removals (path existed via marker; gone after rm).
                if [ ! -e "\$_sd" ]; then
                    _reclaimed_sandboxes=\$((_reclaimed_sandboxes + 1))
                fi
            done
            # Orphan lane-venv reaper [RES-07][OBS-08]: persisted venvs live
            # outside the sandbox so the per-pass wipe leaves them behind. When
            # the sandbox is already gone the marker-gated sweep never sees the
            # pair; reclaim venv+stamp only when all six hold: directory, not
            # this dispatch's key, no sandbox path, lease not live, lock not
            # held, older than SANDBOX_TTL_SEC. Half-pairs are their own leak.
            for _vd in "\$ROOT"/.venv-lane-*/; do
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
                rm -rf "\$_vd" "\$ROOT/.venv-sync-stamp-\$_sk" 2>/dev/null || true
                # Count only real removals (path was a dir above; gone after rm).
                if [ ! -e "\$_vd" ]; then
                    _reclaimed_orphan_venvs=\$((_reclaimed_orphan_venvs + 1))
                fi
            done
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
# Occupancy re-check after lock [CON-11][RES-10]: a prior same-key dispatch may
# have lost its shell (and the lock) while its agent still holds an unexpired
# lease. Never wipe a live sandbox — defer on the exit-75 contract. Malformed
# lease is OCCUPIED (fail-safe). No host probes.
if _lane_occupant_live '${LANE_KEY}'; then
    echo 'remote_agent: same-branch lane still occupying sandbox (${LANE_KEY}) — deferring' >&2
    exit 75
fi
# Declare this dispatch's occupancy before the destructive wipe so a later
# SIGKILL'd peer that loses the lock still advertises the sandbox as live.
_lane_write_live_lease
rm -rf "\$SBX"
mkdir -p "\$SBX"
# Sandbox marker: the ONLY thing that makes a directory a reap candidate.
# Marker-gated (not name-gated) so operator dirs like archive-20260719 are safe.
# mtime of this file is the sandbox sweep's age source.
printf 'lane_key=%s\n' '${LANE_KEY}' > "\$SBX/.workbay-lane-sandbox"
# Per-dispatch outbox OUTSIDE \$SBX: a deferred lane taking the lock and wiping
# \$SBX must not destroy or expose this dispatch's artifacts mid-fetch [CON-12][OBS-08].
OUT_DIR="\$ROOT/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}"
mkdir -p "\$OUT_DIR"
git -C "\$SRC" archive '${LANE_KEY}-${DISPATCH_NONCE}' | tar -x -C "\$SBX"
cd "\$SBX"
git init -q
git config user.email sandbox@grok.invalid
git config user.name grok-sandbox
# Keep sandbox-runtime files out of git so grok's own 'git add -A' cannot
# sweep the brief/schema/logs/marker into its commit and pollute the returned patch.
printf '%s\n' .brief.md .schema.json .grok-result.json .grok-run.log .grok-debug.log .grok-selfverify.json .grok-selfverify.log .venv .workbay-lane-sandbox > .git/info/exclude
git add -A
git -c commit.gpgsign=false commit -q -m 'sandbox base (${LANE_KEY}, history-stripped, remote-severed)'
[ "\$(git remote | wc -l)" -eq 0 ] || { echo 'remote_agent: SANDBOX NOT REMOTE-SEVERED — aborting' >&2; exit 1; }
BASE=\$(git rev-parse HEAD)
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
# Lockfile-hash sync gate (internal S2): skip 'uv sync'
# entirely when uv.lock + every pyproject.toml are byte-identical to the last
# successful sync for this lane AND the persisted venv still exists. The stamp
# lives outside \$SBX (survives the per-pass wipe), keyed by LANE_KEY. Fail-open:
# if sha256sum is unavailable the hash is empty and we always sync (today's
# behavior); any dependency edit changes the hash and forces a re-sync.
SYNC_STAMP="\$ROOT/.venv-sync-stamp-${LANE_KEY}"
_dep_hash=""
if command -v sha256sum >/dev/null 2>&1; then
    _dep_hash=\$( { cat uv.lock 2>/dev/null; find . -name pyproject.toml -not -path './.venv/*' 2>/dev/null | sort | xargs cat 2>/dev/null; } | sha256sum | cut -d' ' -f1 )
fi
if [ -n "\$_dep_hash" ] && [ -d "\$LANE_VENV" ] && [ "\$(cat "\$SYNC_STAMP" 2>/dev/null)" = "\$_dep_hash" ]; then
    echo 'remote_agent: uv.lock+pyproject unchanged for lane — skipping uv sync (warm venv)' >&2
else
    if ! "\$HOME/.local/bin/uv" sync -q >&2 </dev/null 9>&-; then
        echo 'remote_agent: uv sync failed against reused venv — rebuilding fresh and retrying' >&2
        rm -rf "\$LANE_VENV"
        "\$HOME/.local/bin/uv" sync -q >&2 </dev/null 9>&- || { echo 'remote_agent: uv sync failed' >&2; exit 1; }
    fi
    # Stamp the dep hash only AFTER a successful sync so an aborted/failed sync
    # never records a warm-skip for a half-populated venv (fail-open to re-sync).
    [ -n "\$_dep_hash" ] && printf '%s\n' "\$_dep_hash" > "\$SYNC_STAMP" 2>/dev/null || true
fi
# Back-compat: expose the persistent env at the conventional \$SBX/.venv path
# (symlink) so any '.venv/bin'-relative self-verify still resolves. Excluded
# from git above so it cannot pollute grok's patch. Best-effort, non-fatal.
ln -sfn "\$LANE_VENV" "\$SBX/.venv" 2>/dev/null || true
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
_setup_elapsed=\$(( \$(date +%s) - _RP_START ))
_grok_budget=\$(( \$_BOUND_DEADLINE - \$(date +%s) ))
if [ "\$_grok_budget" -le 0 ]; then
    echo "remote_agent: residual timeout exhausted after in-sandbox setup" \
         "(\${_setup_elapsed}s of ${GROK_TIMEOUT}s budget) — deferring lane before grok" >&2
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
        if ( cd "\$SBX" && PATH="\$HOME/.local/bin:\$SBX/.venv/bin:\$PATH" VIRTUAL_ENV="\$SBX/.venv" \$_sv_tw bash -c "\$_sv_cmd" </dev/null 9>&- ) > "\$_sv_log" 2>&1; then _sv_rc=0; else _sv_rc=\$?; fi
        _sv_tail="\$(tail -c 8000 "\$_sv_log" 2>/dev/null || true)"
        # '|| true': a capture-write failure must never abort before format-patch.
        SV_RC="\$_sv_rc" SV_CMD="\$_sv_cmd" SV_TAIL="\$_sv_tail" python3 - > "\$OUT_DIR/.grok-selfverify.json" 2>/dev/null <<'PYEOF' || true
import json, os, re
rc = int(os.environ.get("SV_RC", "1") or "1")
tail = os.environ.get("SV_TAIL", "")
# implementation note S1 [REF-01]: a bool cannot distinguish "your tests failed" from "I could not
# find an interpreter". Emit an outcome enum alongside it. 126 = found-but-not-executable,
# 127 = not-found; bash reports both for a harness fault, not a test result.
if rc == 0:
    outcome = "passed"
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
    "passed": rc == 0,
    "self_verify_outcome": outcome,
    "output_tail": tail,
}))
PYEOF
        echo "remote_agent: off-box self-verify exit \$_sv_rc (patch emitted regardless)" >&2
    fi
fi
}

if ! \$RUNNER \$TW grok --prompt-file .brief.md --cwd . -m '${MODEL}' \
        --json-schema "\$(cat .schema.json)" --max-turns ${MAX_TURNS} \
        --always-approve --no-plan --no-subagents --reasoning-effort '${EFFORT}' \
        --debug-file "\$OUT_DIR/.grok-debug.log" > "\$OUT_DIR/.grok-result.json" 2> .grok-run.log </dev/null 9>&-; then
    # implementation note S3: salvage committed work on exit 3. Own emptiness check against
    # \$BASE (the post-exit-3 check at exit 4 never runs here). When commits exist,
    # emit self-verify capture + format-patch so the host --out redirect is non-empty;
    # rc stays 3. Empty above \$BASE → exit 3 with no patch (honest failure).
    # Salvage BEFORE the fail echo so the producer-message scanner (echo adjacent
    # to exit 3, allowing only tail/cat between) still finds the exit-3 arm.
    if ! git diff --quiet "\$BASE"..HEAD; then
        _emit_off_box_selfverify
        git format-patch "\$BASE"..HEAD --stdout
    fi
    echo 'remote_agent: grok run failed:' >&2
    tail -8 .grok-run.log >&2
    exit 3
fi
if git diff --quiet "\$BASE"..HEAD; then
    echo 'remote_agent: grok produced no committed changes' >&2
    exit 4
fi
# Off-box self-verify (item 26): run the caller's TEST_CMD in the sandbox venv
# AFTER grok's commit, capturing {command,exit_code,passed,output_tail} into
# .grok-selfverify.json (fetched via --selfverify-out). UV_PROJECT_ENVIRONMENT +
# the \$SBX/.venv symlink are already exported above, so a '../../.venv/bin/python'
# or 'uv run' TEST_CMD resolves the lane venv. ALWAYS emit the patch afterwards
# regardless of the result — the commit ports back for salvage/inspection and the
# WORKER (not this script) gates on the captured outcome (never silent-blocks it).
_emit_off_box_selfverify
git format-patch "\$BASE"..HEAD --stdout
REMOTE_EOF
    }

    fetch_result() {
        [ -n "$RESULT_OUT" ] || return 0
        # Best-effort: grok's stdout JSON lives in this dispatch's outbox (outside
        # \$SBX), so fetch it even on a no-change / grok-fail exit — the caller can
        # still surface grok's summary/blockers. Missing file is non-fatal.
        if scp -q -o BatchMode=yes -o ConnectTimeout=10 \
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
        if scp -q -o BatchMode=yes -o ConnectTimeout=10 \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-debug.log" "$DEBUG_OUT" 2>/dev/null; then
            echo "remote_agent: debug log written -> $DEBUG_OUT" >&2
        else
            echo "remote_agent: no debug log fetched (grok emitted no --debug-file?)" >&2
        fi
    }

    fetch_selfverify() {
        [ -n "$SELFVERIFY_OUT" ] || return 0
        # Best-effort, mirroring fetch_result: the off-box self-verify JSON lives in
        # this dispatch's outbox. Missing file is non-fatal — the worker's OBS-08
        # enforcement blocks a commit-landed lane with no capture (never a silent pass).
        if scp -q -o BatchMode=yes -o ConnectTimeout=10 \
                "${REMOTE_HOST}:${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-selfverify.json" "$SELFVERIFY_OUT" 2>/dev/null; then
            echo "remote_agent: self-verify result written -> $SELFVERIFY_OUT" >&2
        else
            echo "remote_agent: no self-verify result fetched (off-box verify not run / no commit?)" >&2
        fi
    }

    rc=0
    _phase "materializing sandbox + dispatching remote grok build (residual ${GROK_TIMEOUT}s)"
    if [ -n "$OUT" ]; then
        if run_patch > "$OUT"; then rc=0; else rc=$?; fi
        if [ "$rc" -eq 0 ]; then
            echo "remote_agent: patch written -> $OUT ($(grep -c '^Subject:' "$OUT") commit(s))" >&2
        fi
    else
        if run_patch; then rc=0; else rc=$?; fi
    fi
    # Defer (75) / host-misconfig (78) lanes must not fetch: a lock-loser has no
    # outbox of its own, and must never pull another dispatch's artifacts [CON-12].
    if [ "$rc" -ne 75 ] && [ "$rc" -ne 78 ]; then
        fetch_result
        fetch_debug
        fetch_selfverify
    fi
    exit "$rc"
    ;;
*)
    sed -n '15,26p' "$0" >&2
    exit 2
    ;;
esac
