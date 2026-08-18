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
#       --agent-spec <file.json> [--out <patch>] \
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
#   --phases-out FILE  fetch the per-dispatch phase-timing record (.grok-phases.json) to
#                      FILE, best-effort (implementation note S1). Fetched unconditionally w.r.t.
#                      exit class so a post-materialize partial still lands on the host;
#                      miss is degrade, never a dispatch error [OBS-08][fail-open].
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
#       · 8 wall-clock bound expiry (deadline reached or timeout(1) status 124)
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

# implementation note S2 — argv placeholder resolver (host-testable; injected into the
# remote body via $(declare -f ...) so there is one source of truth).
# Whole-element substitution only; any '{'/'}' elsewhere → exit 7 [WEB-02].
_agent_spec_resolve_argv() {
    local _i _el _brace_off _el_len _half _ex_start _excerpt
    for _i in "${!AGENT_ARGV[@]}"; do
        _el="${AGENT_ARGV[$_i]}"
        case "$_el" in
            '{brief_file}')
                AGENT_ARGV[$_i]="${AGENT_SPEC_BRIEF_FILE:?agent-spec brief unset}"
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
                # Bounded diagnostic: never dump a multi-kilobyte argv element
                # (cursor-remote carries the full operator prompt as one positional).
                # Walk char-by-char for the first brace offset (avoid %% patterns
                # that embed brace characters and confuse ${...} parsing).
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
                echo "remote_agent: invalid placeholder in argv element: index=${_i} brace_offset=${_brace_off} element_len=${_el_len} excerpt=<${_excerpt}> (cause may be an operator-supplied value such as a cursor-remote prompt, not only the agent-spec recipe)" >&2
                exit 7
                ;;
        esac
    done
}

cmd="${1:-}"; [ "$#" -gt 0 ] && shift

case "$cmd" in
doctor)
    "${SSH[@]}" 'g="$HOME/.grok/bin/grok"
        [ -x "$g" ] && echo "grok    : $("$g" --version)" || echo "grok    : MISSING (install per runbook)"
        [ -f "$HOME/.grok/auth.json" ] && echo "auth    : present (perms $(stat -c %a "$HOME/.grok/auth.json"))" || echo "auth    : MISSING (run: grok login --device-auth)"
        # implementation note: per-backend binary/env paths are AgentSpec data, not doctor
        # hardcodes (vendor-free executor). uv/systemd probes stay host-generic.
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
    BRANCH="" BRIEF="" SCHEMA="" OUT="" RESULT_OUT="" DEBUG_OUT="" TIMEOUT="0" SELFVERIFY_CMD="" SELFVERIFY_OUT="" PHASES_OUT=""
    AGENT_SPEC=""
    AGENT_SPEC_ENABLED=0
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="/dev/null" AGENT_SPEC_STDOUT="{result_file}"
    AGENT_SPEC_STDERR="{run_log}" AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="9"
    AGENT_SPEC_ENV_FILE=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --branch) BRANCH="${2:-}"; shift 2 ;;
            --brief) BRIEF="${2:-}"; shift 2 ;;
            --schema) SCHEMA="${2:-}"; shift 2 ;;
            --out) OUT="${2:-}"; shift 2 ;;
            --result-out) RESULT_OUT="${2:-}"; shift 2 ;;
            --debug-out) DEBUG_OUT="${2:-}"; shift 2 ;;
            --timeout) TIMEOUT="${2:-}"; shift 2 ;;
            --test-cmd) SELFVERIFY_CMD="${2:-}"; shift 2 ;;
            --selfverify-out) SELFVERIFY_OUT="${2:-}"; shift 2 ;;
            --phases-out) PHASES_OUT="${2:-}"; shift 2 ;;
            --agent-spec) AGENT_SPEC="${2:-}"; shift 2 ;;
            --model|--max-turns|--effort)
                die "--agent-spec required; legacy $1 is removed (implementation note S4)"
                ;;
            *) die "unknown arg: $1" ;;
        esac
    done
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
_out.close()
PY
    then
        rm -f "$_meta_tmp"
        die "--agent-spec metadata rejected (see stderr)"
    fi
    # Fixed-position NUL load (same discipline as the argv sidecar). 8 fields.
    AGENT_SPEC_BIN="" AGENT_SPEC_STDIN="" AGENT_SPEC_STDOUT="" AGENT_SPEC_STDERR=""
    AGENT_SPEC_PATH_PREPEND="" AGENT_SPEC_CLOSE_FDS="" AGENT_SPEC_REQUIRES_TIMEOUT=""
    AGENT_SPEC_ENV_FILE=""
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
            *)
                rm -f "$_meta_tmp"
                die "--agent-spec metadata: too many fields"
                ;;
        esac
        _meta_i=$((_meta_i + 1))
    done < "$_meta_tmp"
    rm -f "$_meta_tmp"
    [ "$_meta_i" -eq 8 ] || die "--agent-spec metadata field count ${_meta_i} (need 8)"
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
                \"\$HOME/${AGENT_ROOT}/.lane-out-${LANE_KEY}-${DISPATCH_NONCE}/.grok-phases.json\" \
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
    # implementation note S1: transport = push only (absolute integer Unix seconds).
    _transport_start_ts="$(date +%s)"
    GIT_SSH_COMMAND='ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=4' \
        git push --quiet --force "${REMOTE_HOST}:${REMOTE_DIR}" "${BRANCH}:refs/heads/${LANE_KEY}-${DISPATCH_NONCE}" >&2
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
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$BRIEF"  "${REMOTE_HOST}:${AGENT_ROOT}/.brief-${LANE_KEY}-${DISPATCH_NONCE}.md"   >&2
    scp -q -o BatchMode=yes -o ConnectTimeout=10 "$SCHEMA" "${REMOTE_HOST}:${AGENT_ROOT}/.schema-${LANE_KEY}-${DISPATCH_NONCE}.json" >&2
    if [ -n "${AGENT_SPEC:-}" ]; then
        # JSON metadata (auth_match / result_source) + NUL argv sidecar.
        case "$AGENT_SPEC" in
            *.json) _agent_spec_json="$AGENT_SPEC" ;;
            *) _agent_spec_json="${AGENT_SPEC}.json" ;;
        esac
        [ -f "$_agent_spec_json" ] || die "--agent-spec json not found: ${_agent_spec_json}"
        scp -q -o BatchMode=yes -o ConnectTimeout=10 "$_agent_spec_json" \
            "${REMOTE_HOST}:${AGENT_ROOT}/.agent-spec-${LANE_KEY}-${DISPATCH_NONCE}.json" >&2
        scp -q -o BatchMode=yes -o ConnectTimeout=10 "$AGENT_SPEC_ARGV" \
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
    run_patch() {
        # implementation note: capture resolver source once so the unquoted heredoc can
        # inject it via an allowlisted host expansion (not a bare $(...) site).
        _AGENT_SPEC_RESOLVER_SRC="$(declare -f _agent_spec_resolve_argv)"
        # implementation note S1 / REV0192R12-B-1: ssh is a grandchild of the adapter.
        # Only this script can honestly stamp call/return around the heredoc.
        _emit_host_instant ssh_call_ts "$(date +%s)"
        # Capture rc without set -e abort so ssh_return_ts still stamps [fail-open].
        _run_patch_rc=0
        # shellcheck disable=SC2029
        "${SSH[@]}" bash -s <<REMOTE_EOF || _run_patch_rc=$?
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
# Fail-open phase record on any post-outbox exit (partial or complete) [OBS-08].
# Bash EXIT traps REPLACE (they do not stack). Chain lease clear so a post-
# materialize exit still removes the occupancy lease — otherwise the host
# spares the outbox and non-venv artifacts persist across the pass boundary
# [REV0192S1-A-01] (operator security constraint: only derived venvs may persist).
trap '_emit_phases_record || true; _lane_clear_live_lease' EXIT
# remote_preamble closes at archive_extract start; both use the VM clock.
_archive_start=\$(date +%s)
_phase_record remote_preamble "\$_RP_ENTRY" "\$_archive_start"
git -C "\$SRC" archive '${LANE_KEY}-${DISPATCH_NONCE}' | tar -x -C "\$SBX"
_archive_end=\$(date +%s)
_phase_record archive_extract "\$_archive_start" "\$_archive_end"
cd "\$SBX"
_git_init_start=\$(date +%s)
git init -q
git config user.email sandbox@grok.invalid
git config user.name grok-sandbox
# Keep sandbox-runtime files out of git so grok's own 'git add -A' cannot
# sweep the brief/schema/logs/marker into its commit and pollute the returned patch.
printf '%s\n' .brief.md .schema.json .grok-result.json .grok-run.log .grok-debug.log .grok-selfverify.json .grok-selfverify.log .venv .workbay-lane-sandbox .agent-stream.jsonl > .git/info/exclude
git add -A
git -c commit.gpgsign=false commit -q -m 'sandbox base (${LANE_KEY}, history-stripped, remote-severed)'
[ "\$(git remote | wc -l)" -eq 0 ] || { echo 'remote_agent: SANDBOX NOT REMOTE-SEVERED — aborting' >&2; exit 1; }
_git_init_end=\$(date +%s)
_phase_record git_init "\$_git_init_start" "\$_git_init_end"
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
_sync_start=\$(date +%s)
if command -v sha256sum >/dev/null 2>&1; then
    _dep_hash=\$( { cat uv.lock 2>/dev/null; find . -name pyproject.toml -not -path './.venv/*' 2>/dev/null | sort | xargs cat 2>/dev/null; } | sha256sum | cut -d' ' -f1 )
fi
if [ -n "\$_dep_hash" ] && [ -d "\$LANE_VENV" ] && [ "\$(cat "\$SYNC_STAMP" 2>/dev/null)" = "\$_dep_hash" ]; then
    echo 'remote_agent: uv.lock+pyproject unchanged for lane — skipping uv sync (warm venv)' >&2
    _PHASES_WARM_SKIP=1
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
_sync_end=\$(date +%s)
_phase_record sync "\$_sync_start" "\$_sync_end"
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
    "\$HOME/.local/bin/uv" sync -q >&2 </dev/null 9>&- || {
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
_grok_budget=\$(( \$_BOUND_DEADLINE - \$(date +%s) ))
if [ "\$_grok_budget" -le 0 ]; then
    # Post-materialize partial: expensive cold sample must be written + fetched.
    # Emit BEFORE the diagnostic echo so exit-75 producer walkers still resolve
    # the human-readable message adjacent to exit 75 [REV0192S1-AB-01].
    _PHASES_PARTIAL=1
    _emit_phases_record || true
    echo "remote_agent: residual timeout exhausted after in-sandbox setup" \
         "(\${_vm_span_elapsed}s of ${GROK_TIMEOUT}s budget) — deferring lane before grok" >&2
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
    if [ "\$(date +%s)" -ge "\$_BOUND_DEADLINE" ] || [ "\$_agent_rc" -eq 124 ]; then
        printf '%s\n' 8
        return 0
    fi
    printf '%s\n' 3
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

# Salvage committed sandbox work on failure arms (finding 14073): when HEAD has
# diverged from BASE, emit the off-box self-verify record and stream commits
# back as a patch so the operator does not lose work. Shared by result_degraded,
# result_rewrite_failed, agent_failed, the catch-all unknown-status arm, and the
# EXIT trap (OFFLOAD-EXIT1-NO-STDERR-TAIL-01). auth_failed is exempt — auth
# match fails before the agent runs.
# DURFIX-SALVAGE-EMPTY-PATCH-01: empty format-patch while HEAD != BASE is a
# loud failure (return 1 + stderr), never a silent success-looking 0-byte patch.
_SALVAGE_DONE=0
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
_harvest_gitdir_structure_checks() {
    # A file or symlink .git is a gitdir: pointer (or worse). Path checks
    # against .git/... no-op and git follows the pointer to an attacker gitdir.
    if [ -L .git ]; then
        _harvest_tamper_abort || return 1
    fi
    if [ -e .git ] && [ ! -d .git ]; then
        _harvest_tamper_abort || return 1
    fi
    if [ -e .git/objects/info/alternates ] || [ -e .git/objects/info/http-alternates ] || [ -e .git/commondir ]; then
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
    _obj_links=\$(find .git/objects -type l -print 2>/dev/null) || _find_rc=\$?
    if [ "\$_find_rc" -ne 0 ] || [ -n "\$_obj_links" ]; then
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
    # Harvest git must never lazy-fetch or speak file/ext/ssh protocols.
    export GIT_NO_LAZY_FETCH=1
    # Malformed config: git leftover-scan would ``|| true`` fail-open.
    if ! git config --file .git/config --name-only --list >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    # Promisor / partialclone / insteadOf / credential / protocol / fetch
    # keys are not neutralized — any presence is fail-closed. Regex is
    # lowercase because ``git config --name-only`` emits canonical keys
    # (``core.hookspath``) and --get-regexp is case-sensitive.
    _forb=\$(git config --file .git/config --name-only --get-regexp '^(remote\.|extensions\.|credential\.|uploadpack\.|fetch\.|url\..*\.insteadof|protocol\..*\.allow)' 2>/dev/null || true)
    if [ -n "\$_forb" ]; then
        _harvest_tamper_abort || return 1
    fi
    # Neutralize includes FIRST so [include]/includeIf cannot re-introduce
    # a driver that survives the later --unset / --remove-section.
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --remove-section include 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all include.path 2>/dev/null || true
    _incif_keys=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --name-only --get-regexp '^includeIf\..*\.path\$' 2>/dev/null || true)
    if [ -n "\$_incif_keys" ]; then
        printf '%s\n' "\$_incif_keys" | while IFS= read -r _ik; do
            [ -n "\$_ik" ] || continue
            GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all "\$_ik" 2>/dev/null || true
        done
    fi
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all core.fsmonitor 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all core.hooksPath 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all core.sshCommand 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all diff.external 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --remove-section diff 2>/dev/null || true
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --remove-section filter 2>/dev/null || true
    _df_keys=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --name-only --get-regexp '^(diff|filter)\.' 2>/dev/null || true)
    if [ -n "\$_df_keys" ]; then
        printf '%s\n' "\$_df_keys" | while IFS= read -r _dk; do
            [ -n "\$_dk" ] || continue
            GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null config --unset-all "\$_dk" 2>/dev/null || true
        done
    fi
    return 0
}
_harvest_gitdir_leftover_and_raw_scans() {
    # Read the file itself — ``git -c core.fsmonitor=false`` would otherwise
    # make --get-regexp report our own hardening override as a leftover key.
    # Lowercase regex: git emits ``core.hookspath``; camelCase missed leftovers.
    _left=\$(git config --file .git/config --name-only --get-regexp '^(include\.|includeif\.|core\.hookspath|core\.sshcommand|core\.fsmonitor|diff\.|filter\.|remote\.|extensions\.|credential\.|uploadpack\.|fetch\.|url\..*\.insteadof|protocol\..*\.allow)' 2>/dev/null || true)
    if [ -n "\$_left" ]; then
        _harvest_tamper_abort || return 1
    fi
    # Raw-file scan: case-insensitive + line-continuation tolerant. A
    # ``hooks\\`` + ``Path`` split (or ``[Remote]``) that git's parser
    # rejects must still fail-closed, not skip the leftover check.
    if ! command -v sed >/dev/null 2>&1 || ! command -v grep >/dev/null 2>&1; then
        _harvest_tamper_abort || return 1
    fi
    _raw_cfg=\$(sed -e ':a' -e '/\\\\\$/N' -e 's/\\\\\\n//' -e 'ta' .git/config 2>/dev/null || true)
    if printf '%s\n' "\$_raw_cfg" | grep -qiE '^[[:space:]]*\[(remote|extensions|credential|uploadpack|fetch|url|protocol|include|includeif|filter)([].[:space:]]|$)' \
        || printf '%s\n' "\$_raw_cfg" | grep -qiE '^[[:space:]]*(hookspath|sshcommand|fsmonitor|insteadof)[[:space:]]*=' \
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
_salvage_committed_work() {
    [ "\${_SALVAGE_DONE:-0}" -eq 1 ] && return 0
    [ -n "\${BASE:-}" ] || return 0
    if ! _sanitize_harvest_gitdir; then
        HARVEST_GITDIR_UNSAFE=1
        _STDERR_TAIL_DONE=1
        _SALVAGE_DONE=1
        echo 'remote_agent: gitdir tampering detected' >&2
        exit 3
    fi
    GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --verify HEAD >/dev/null 2>&1 || return 0
    if GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD 2>/dev/null; then
        return 0
    fi
    _SALVAGE_DONE=1
    _emit_off_box_selfverify
    _salv_tmp=\$(mktemp "\${TMPDIR:-/tmp}/ra-salvage.XXXXXX") || {
        echo 'remote_agent: salvage mktemp failed while HEAD != BASE' >&2
        return 1
    }
    if ! GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null format-patch --no-textconv "\$BASE"..HEAD --stdout > "\$_salv_tmp"; then
        echo 'remote_agent: salvage format-patch failed while HEAD != BASE' >&2
        rm -f "\$_salv_tmp"
        return 1
    fi
    if [ ! -s "\$_salv_tmp" ]; then
        echo 'remote_agent: salvage format-patch empty while HEAD != BASE (commits exist but patch is 0 bytes)' >&2
        echo "remote_agent: BASE=\$BASE HEAD=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse HEAD 2>/dev/null) branch=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --abbrev-ref HEAD 2>/dev/null)" >&2
        rm -f "\$_salv_tmp"
        return 1
    fi
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
    if [ -n "\${BASE:-}" ] && GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --verify HEAD >/dev/null 2>&1; then
        if ! GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD 2>/dev/null; then
            echo "remote_agent: sandbox HEAD diverges from BASE (HEAD=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --short HEAD) BASE=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --short "\$BASE")) — salvage should have emitted a patch" >&2
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
    _agent_rc=0
    if (
        if [ -n "\$_envf" ] && [ -f "\$_envf" ]; then
            set -a
            # shellcheck disable=SC1090
            . "\$_envf"
            set +a
        fi
        exec \$RUNNER \$TW '${AGENT_SPEC_BIN}' "\${AGENT_ARGV[@]}" \
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
patterns = [str(p) for p in (auth.get("patterns") or []) if str(p)]
exit_codes = {int(x) for x in (auth.get("exit_codes") or [])}
streams = auth.get("streams")
if streams is None and auth.get("stream"):
    streams = [auth.get("stream")]
if not isinstance(streams, list):
    streams = []
stream_text = []
for name in streams:
    path = stdout_path if name == "stdout" else stderr_path if name == "stderr" else ""
    if not path:
        continue
    try:
        stream_text.append(Path(path).read_text(encoding="utf-8", errors="replace"))
    except OSError as exc:
        # Loud degrade for auth-stream IO (do not silently shrink the blob).
        print(f"remote_agent: auth stream unreadable ({name}): {exc}", file=sys.stderr)
        continue
blob = "\n".join(stream_text)
# precedence: exit_codes_then_patterns (implementation note D6)
if agent_rc in exit_codes:
    print("auth_failed")
    raise SystemExit(0)
if patterns and any(p in blob for p in patterns):
    print("auth_failed")
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
        _write_degraded(raw)
        print("result_degraded")
        raise SystemExit(0)

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
    shaped = recovered is not None
    if not shaped:
        # Tier two (unshaped): well-formed dict that failed the shape gate but
        # still carries worker/result keys. Keep the payload so a committed
        # turn's summary/tests_run survive; the orchestrator clamps
        # handoff_action fail-closed. Do NOT collapse this into unparseable.
        # Source of truth: _result_text.py _RESULTISH_KEYS (VM cannot import).
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
        auth_failed)
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: auth_match failure (exit 6)' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            exit 6
            ;;
        result_degraded)
            _salvage_committed_work || true
            echo 'remote_agent: result present but unparseable (exit 5)' >&2
            exit 5
            ;;
        result_rewrite_failed)
            _salvage_committed_work || true
            echo 'remote_agent: result rewrite failed after multi-object salvage (exit 5)' >&2
            exit 5
            ;;
        agent_failed)
            _salvage_committed_work || true
            _STDERR_TAIL_DONE=1
            echo 'remote_agent: agent run failed:' >&2
            tail -8 "\$_spec_stderr" >&2 || true
            # Wall-clock expiry is its own transport status (exit 8);
            # _salvage_committed_work (git format-patch when BASE..HEAD diverges)
            # still runs so partial commits return from an expired lane.
            exit "\$(_classify_agent_failed_exit)"
            ;;
        ok) ;;
        *)
            _salvage_committed_work || true
            echo "remote_agent: post-classify unknown status: \${_post}" >&2
            exit 3
            ;;
    esac
    if ! _sanitize_harvest_gitdir; then
        HARVEST_GITDIR_UNSAFE=1
        _STDERR_TAIL_DONE=1
        echo 'remote_agent: gitdir tampering detected' >&2
        exit 3
    fi
    if GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null diff --quiet --no-textconv --no-ext-diff "\$BASE"..HEAD; then
        echo 'remote_agent: agent produced no committed changes' >&2
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
    if ! GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null format-patch --no-textconv "\$BASE"..HEAD --stdout > "\$_ok_patch_tmp"; then
        echo 'remote_agent: format-patch failed while HEAD != BASE (exit 3)' >&2
        rm -f "\$_ok_patch_tmp"
        exit 3
    fi
    if [ ! -s "\$_ok_patch_tmp" ]; then
        echo 'remote_agent: format-patch empty while HEAD != BASE (exit 3)' >&2
        echo "remote_agent: BASE=\$BASE HEAD=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse HEAD) branch=\$(GIT_NO_LAZY_FETCH=1 git -c protocol.file.allow=never -c protocol.ext.allow=never -c protocol.ssh.allow=never -c uploadpack.allowFilter=false -c core.fsmonitor=false -c core.hooksPath=/dev/null rev-parse --abbrev-ref HEAD)" >&2
        rm -f "\$_ok_patch_tmp"
        exit 3
    fi
    _SALVAGE_DONE=1
    cat "\$_ok_patch_tmp"
    rm -f "\$_ok_patch_tmp"
    exit 0
fi
# <<< agent-exec
echo 'remote_agent: --agent-spec required (legacy no-spec path deleted)' >&2
exit 2
REMOTE_EOF
        # Immediately after heredoc returns — before any artifact fetch [REV0192R12-B-1].
        _emit_host_instant ssh_return_ts "$(date +%s)"
        return "$_run_patch_rc"
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

    # implementation note S1: phases fetch is script-owned. Remote path is nonce-scoped
    # [CON-12]; $OUT_DIR is VM-only — never reference it on the local side.
    fetch_phases() {
        [ -n "$PHASES_OUT" ] || return 0
        # Stage into a temp path then rename so a failed scp cannot leave a
        # prior non-empty PHASES_OUT looking like this dispatch's ok [A-04].
        _phases_fetch_tmp="${PHASES_OUT}.fetch.$$"
        if scp -q -o BatchMode=yes -o ConnectTimeout=10 \
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
    # Three-way fetch gate (S1.4): fetch_phases is unconditional w.r.t. rc so a
    # post-materialize exit-75 partial still lands; result/debug/selfverify stay
    # behind the existing 75/78 gate (lock-loser must not pull another's outbox).
    _phases_fetch_rc=0
    if fetch_phases; then _phases_fetch_rc=0; else _phases_fetch_rc=$?; fi
    if [ "$rc" -ne 75 ] && [ "$rc" -ne 78 ]; then
        fetch_result
        fetch_debug
        fetch_selfverify
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
    ;;
*)
    sed -n '15,26p' "$0" >&2
    exit 2
    ;;
esac
