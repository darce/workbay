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
#   --timeout SECONDS  hard wall-clock budget for the remote turn (0 = none), measured
#                      from build start. Pre-dispatch probe + push + scp consume part
#                      of this budget; residual = max(0, budget − elapsed) is applied
#                      to remote grok (never floored above the remaining budget — when
#                      residual hits 0 the turn fails fast before grok starts). Caller
#                      should already subtract post-grok fetch headroom so result fetch
#                      still fits under the local transport bound (RES-02). Requires
#                      `timeout` on the VM, else skipped.
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
#                      (default 3, must be an integer >= 1); at/above the cap the
#                      lane defers (exit 75).
#
# Security: `git archive` ships TRACKED files only (no gitignored secrets) ->
# fresh `git init` -> ONE synthetic commit -> NO remote, so grok has no history,
# secrets, or remote to exfiltrate. GROK_ZDR_ENABLED=1 gates uploads too. The
# script asserts the sandbox is remote-severed before running grok.
#
# Exit: 0 patch produced · 3 grok run failed · 4 no committed changes ·
#       75 retryable defer (VM memory floor, lane cap, or residual-timeout
#       exhausted pre-grok) · 78 host not configured · 2 usage/validation error.
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
# VM MemAvailable floor (MiB): defer the lane when the VM is below this, reserving
# headroom for ALL non-lane work on the box (co-resident mission-critical procs).
MEM_FLOOR_MB="${_env_mem_floor:-2048}"
# Concurrent named grok-lane-* scopes on the VM (implementation note S5; [RES-14] backpressure).
# Default 3 reserves headroom for co-resident mission-critical procs. Must be >= 1
# (MAX_LANES=0 would permanently defer every turn with no useful signal).
MAX_LANES="${_env_max_lanes:-3}"

# validation (interpolated into the remote shell)
case "$REMOTE_DIR" in ""|.|/*|*..*|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid REMOTE_DIR" >&2; exit 2 ;; esac
case "$AGENT_ROOT" in ""|/*|*..*|*[!A-Za-z0-9/_.-]*) echo "remote_agent: invalid AGENT_ROOT" >&2; exit 2 ;; esac
case "$MEM_MAX" in *[!0-9GMK]*|"") echo "remote_agent: MEMORY_MAX must look like 6G/512M" >&2; exit 2 ;; esac
case "$CPU_QUOTA" in *[!0-9%]*|"") echo "remote_agent: CPU_QUOTA must look like 200%" >&2; exit 2 ;; esac
case "$MEM_FLOOR_MB" in *[!0-9]*|"") echo "remote_agent: MEM_FLOOR_MB must be an integer (MiB)" >&2; exit 2 ;; esac
case "$MAX_LANES" in *[!0-9]*|"") echo "remote_agent: MAX_LANES must be an integer >= 1" >&2; exit 2 ;; esac
if [ "$MAX_LANES" -lt 1 ]; then
    echo "remote_agent: MAX_LANES must be an integer >= 1" >&2
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
        command -v systemd-run >/dev/null && echo "caps    : systemd-run available" || echo "caps    : systemd-run MISSING (nice/ionice only)"'
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
    BRANCH="" BRIEF="" SCHEMA="" MODEL="grok-4.5" MAX_TURNS="40" EFFORT="high" OUT="" RESULT_OUT="" DEBUG_OUT="" TIMEOUT="0"
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
    # Named systemd scope unit: grok-lane-<LANE_KEY> so active lanes are
    # countable + debuggable ([RES-14] concurrency ceiling; implementation note S5).
    LANE_UNIT="grok-lane-${LANE_KEY}"

    # Wall-clock for residual --timeout after pre-dispatch work (probe/push/scp).
    BUILD_START_TS="$(date +%s)"
    # Per-phase progress with elapsed seconds (implementation note observability delta): a
    # future stall now names its phase + duration instead of a silent timeout.
    _phase() { echo "remote_agent: [+$(( $(date +%s) - BUILD_START_TS ))s] $*" >&2; }

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
    # Push target is LANE_KEY (not basename): basename-derived refs collided for
    # distinct full branch names; LANE_KEY is collision-proof. The full $BRANCH
    # source ref is preserved as the local side of the refspec.
    _phase "pushing $BRANCH -> ${REMOTE_HOST}:${REMOTE_DIR} (refs/heads/${LANE_KEY})"
    # BatchMode/ConnectTimeout (matching the SSH array): the push must FAIL FAST,
    # never prompt or hang, even if a caller leaves stdin attached — belt to the
    # `exec </dev/null` above (implementation note).
    GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=4' \
        git push --quiet --force "${REMOTE_HOST}:${REMOTE_DIR}" "${BRANCH}:refs/heads/${LANE_KEY}" >&2

    # 2) ship brief + schema to the sandbox PARENT (survives the sandbox wipe)
    "${SSH[@]}" "mkdir -p \"\$HOME/${AGENT_ROOT}\"" >&2
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$BRIEF"  "${REMOTE_HOST}:${AGENT_ROOT}/.brief-${LANE_KEY}.md"   >&2
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$SCHEMA" "${REMOTE_HOST}:${AGENT_ROOT}/.schema-${LANE_KEY}.json" >&2

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
SRC="\$HOME/${REMOTE_DIR}"
ROOT="\$HOME/${AGENT_ROOT}"
SBX="\$ROOT/${LANE_KEY}"
rm -rf "\$SBX"
mkdir -p "\$SBX"
git -C "\$SRC" archive '${LANE_KEY}' | tar -x -C "\$SBX"
cd "\$SBX"
git init -q
git config user.email sandbox@grok.invalid
git config user.name grok-sandbox
# Keep sandbox-runtime files out of git so grok's own 'git add -A' cannot
# sweep the brief/schema/logs into its commit and pollute the returned patch.
printf '%s\n' .brief.md .schema.json .grok-result.json .grok-run.log .grok-debug.log > .git/info/exclude
git add -A
git -c commit.gpgsign=false commit -q -m 'sandbox base (${LANE_KEY}, history-stripped, remote-severed)'
[ "\$(git remote | wc -l)" -eq 0 ] || { echo 'remote_agent: SANDBOX NOT REMOTE-SEVERED — aborting' >&2; exit 1; }
BASE=\$(git rev-parse HEAD)
cp "\$ROOT/.brief-${LANE_KEY}.md" .brief.md
cp "\$ROOT/.schema-${LANE_KEY}.json" .schema.json
# </dev/null: uv inherits the bash -s script stream on fd0 like grok does —
# a stdin-reading child would eat the unread script tail (silent truncation).
"\$HOME/.local/bin/uv" sync -q >&2 </dev/null || { echo 'remote_agent: uv sync failed' >&2; exit 1; }
RUNNER='nice -n 10 ionice -c3'
# An OOM-killed prior run leaves ${LANE_UNIT} in systemd 'failed' state,
# which refuses the unit name on the next run of the same lane — clear it
# first (no-op when absent).
systemctl --user reset-failed ${LANE_UNIT} 2>/dev/null || true
if systemd-run --quiet --user --scope -p MemoryMax=${MEM_MAX} true 2>/dev/null; then
    # Named per-lane scope (grok-lane-<LANE_KEY>) so active lanes are countable.
    RUNNER="systemd-run --quiet --user --scope --unit ${LANE_UNIT} -p MemoryMax=${MEM_MAX} -p CPUQuota=${CPU_QUOTA} nice -n 10 ionice -c3"
fi
# Residual wall-clock bound on grok (RES-02): pre-dispatch cost already
# subtracted into GROK_TIMEOUT so hung remote grok still self-terminates
# before the caller's local bound. Skipped when timeout(1) absent or 0.
TW=''
if command -v timeout >/dev/null 2>&1 && [ '${GROK_TIMEOUT}' -gt 0 ] 2>/dev/null; then
    TW='timeout -k 10 ${GROK_TIMEOUT}'
fi
# Redirect grok stdin from /dev/null: this remote body is fed to `bash -s`
# on the same stdin the child inherits. A stdin-reading grok would eat the
# script tail (no-commit check + git format-patch never run; ssh returns 0
# with an empty "success" patch).
if ! \$RUNNER \$TW grok --prompt-file .brief.md --cwd . -m '${MODEL}' \
        --json-schema "\$(cat .schema.json)" --max-turns ${MAX_TURNS} \
        --always-approve --no-plan --no-subagents --reasoning-effort '${EFFORT}' \
        --debug-file .grok-debug.log > .grok-result.json 2> .grok-run.log </dev/null; then
    echo 'remote_agent: grok run failed:' >&2
    tail -8 .grok-run.log >&2
    exit 3
fi
if git diff --quiet "\$BASE"..HEAD; then
    echo 'remote_agent: grok produced no committed changes' >&2
    exit 4
fi
git format-patch "\$BASE"..HEAD --stdout
REMOTE_EOF
    }

    fetch_result() {
        [ -n "$RESULT_OUT" ] || return 0
        # Best-effort: grok's stdout JSON persists in the sandbox until the next run
        # for this lane wipes it, so fetch it even on a no-change / grok-fail exit —
        # the caller can still surface grok's summary/blockers. Missing file is non-fatal.
        if scp -q -o BatchMode=yes -o ConnectTimeout=10 \
                "${REMOTE_HOST}:${AGENT_ROOT}/${LANE_KEY}/.grok-result.json" "$RESULT_OUT" 2>/dev/null; then
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
                "${REMOTE_HOST}:${AGENT_ROOT}/${LANE_KEY}/.grok-debug.log" "$DEBUG_OUT" 2>/dev/null; then
            echo "remote_agent: debug log written -> $DEBUG_OUT" >&2
        else
            echo "remote_agent: no debug log fetched (grok emitted no --debug-file?)" >&2
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
    fetch_result
    fetch_debug
    exit "$rc"
    ;;
*)
    sed -n '15,26p' "$0" >&2
    exit 2
    ;;
esac
