#!/bin/sh
# Bounded interpreter fallback for hook launches (POSIX /bin/sh only).
# Select a viable Python, then exec the existing _run_guard.py (argv remainder)
# exactly once with stdin/stdout/stderr inherited.

PROBE_SECS=1
GRACE_SECS=1
# Finite yield cap for parent arm-wait. See _probe_viable comment.
ARM_READY_YIELDS=4096

_fail_before_python() {
    for _arg in "$@"; do
        case "$_arg" in
            --fail-mode=closed) exit 2 ;;
        esac
    done
    exit 0
}

_is_pid() {
    case "$1" in
        ''|*[!0-9]*) return 1 ;;
    esac
    [ "$1" -gt 1 ]
}

# Snapshot descendants of $_root_pid (inclusive), descendants first.
# Bounded passes over a portable `ps -e -o pid= -o ppid=` listing.
# Never includes the launcher pid or this helper's pid.
_descendant_list() {
    _root_pid=$1
    _self_pid=$2
    _launch_pid=$3
    _snap=$4
    _list=
    _known="|${_root_pid}|"
    _pass=0
    while [ "$_pass" -lt 8 ]; do
        _added=
        while IFS= read -r _line || [ -n "$_line" ]; do
            set -- $_line
            [ $# -ge 2 ] || continue
            _cpid=$1
            _cppid=$2
            _is_pid "$_cpid" || continue
            _is_pid "$_cppid" || continue
            [ "$_cpid" -eq "$_self_pid" ] && continue
            [ "$_cpid" -eq "$_launch_pid" ] && continue
            [ "$_cpid" -eq "$_root_pid" ] && continue
            case "$_known" in
                *"|${_cppid}|"*)
                    case "$_known" in
                        *"|${_cpid}|"*) ;;
                        *)
                            _known="${_known}${_cpid}|"
                            _added=1
                            _list="${_cpid} ${_list}"
                            ;;
                    esac
                    ;;
            esac
        done < "$_snap"
        [ -n "$_added" ] || break
        _pass=$((_pass + 1))
    done
    printf '%s\n' "${_list}${_root_pid}"
}

_signal_list() {
    _sig=$1
    shift
    for _p in "$@"; do
        _is_pid "$_p" || continue
        [ "$_p" -eq "$$" ] && continue
        kill -s "$_sig" "$_p" 2>/dev/null
    done
}

# Tiny probe: stdin from /dev/null, finite deadline, no leftover watchdog.
# Isolated interpreter (-I) so PYTHONPATH / user site cannot run during probe.
# After the deadline, TERM the candidate tree, short grace, then KILL/reap.
# Disarm live before killing/reaping the watchdog so a late kill cannot hit a
# reused probe PID; then reap the watchdog and drop the private directory.
#
# Watchdog arm handshake: the child writes a ready token only after
# `_timer=$!` and the TERM/HUP/INT trap are armed. The parent cancels
# the watchdog only after that token exists. Arm-wait is a bounded
# yield loop (ARM_READY_YIELDS, 4096) of `command -p sleep 0` plus
# ready-file and watchdog-liveness checks — not a multi-million
# busy-spin (starves the watchdog) and not `sleep 1` (too coarse for
# the 1s probe). 4096 yields is typically milliseconds on immediate
# readiness and is large enough that a 200000-iteration arithmetic
# gap in the watchdog (deterministic delayed-arm fixture) can
# schedule without starving it. Cap or a dead watchdog uses a
# distinct arm-failure cleanup (snapshot trees, descendant-first
# TERM then KILL, no deadline grace sleep) and returns nonviable.
_probe_viable() {
    _interp=$1
    _launch_pid=$$
    _old_umask=$(umask)
    umask 077
    _work=
    _i=0
    while [ "$_i" -lt 32 ]; do
        _try="/tmp/wb-lp-$$-${_i}"
        if command -p mkdir "$_try" 2>/dev/null; then
            _work=$_try
            break
        fi
        _i=$((_i + 1))
    done
    umask "$_old_umask"
    [ -n "$_work" ] || return 1

    : > "$_work/live" || {
        command -p rmdir "$_work" 2>/dev/null
        return 1
    }

    "$_interp" -I -c 'raise SystemExit(0)' </dev/null >/dev/null 2>&1 &
    _pid=$!
    printf '%s\n' "$_pid" > "$_work/pid"

    (
        _timer=
        _reap_timer() {
            if _is_pid "$_timer"; then
                kill "$_timer" 2>/dev/null
                wait "$_timer" 2>/dev/null
            fi
            _timer=
            exit 0
        }
        trap '_reap_timer' HUP INT TERM
        command -p sleep "$PROBE_SECS" &
        _timer=$!
        : > "$_work/ready" || exit 1
        wait "$_timer"
        _timer=
        trap - HUP INT TERM
        if [ -f "$_work/live" ]; then
            _old=
            IFS= read -r _old < "$_work/pid" || _old=
            if _is_pid "$_old"; then
                command -p ps -e -o pid= -o ppid= > "$_work/ps" 2>/dev/null || :
                _tree=$(_descendant_list "$_old" "$$" "$_launch_pid" "$_work/ps")
                set -- $_tree
                _signal_list TERM "$@"
                command -p sleep "$GRACE_SECS"
                if [ -f "$_work/live" ]; then
                    command -p ps -e -o pid= -o ppid= > "$_work/ps" 2>/dev/null || :
                    _tree=$(_descendant_list "$_old" "$$" "$_launch_pid" "$_work/ps")
                    set -- $_tree
                    _signal_list KILL "$@"
                    kill -s KILL "$_old" 2>/dev/null
                fi
            fi
        fi
    ) &
    _wd=$!

    _armed=
    _arm_i=0
    while [ "$_arm_i" -lt "$ARM_READY_YIELDS" ]; do
        if [ -f "$_work/ready" ]; then
            _armed=1
            break
        fi
        if ! kill -0 "$_wd" 2>/dev/null; then
            break
        fi
        command -p sleep 0
        _arm_i=$((_arm_i + 1))
    done

    if [ -z "$_armed" ]; then
        command -p ps -e -o pid= -o ppid= > "$_work/ps" 2>/dev/null || :
        _tree_wd=$(_descendant_list "$_wd" "$$" "$_launch_pid" "$_work/ps")
        _tree_pr=$(_descendant_list "$_pid" "$$" "$_launch_pid" "$_work/ps")
        set -- $_tree_wd $_tree_pr
        _signal_list TERM "$@"
        _signal_list KILL "$@"
        if _is_pid "$_wd"; then
            kill -s KILL "$_wd" 2>/dev/null
            wait "$_wd" 2>/dev/null
        fi
        if _is_pid "$_pid"; then
            kill -s KILL "$_pid" 2>/dev/null
            wait "$_pid" 2>/dev/null
        fi
        command -p rm -f "$_work/live" "$_work/pid" "$_work/ps" "$_work/ready"
        command -p rmdir "$_work" 2>/dev/null
        return 1
    fi

    wait "$_pid"
    _rc=$?

    command -p rm -f "$_work/live" "$_work/pid" "$_work/ps" "$_work/ready"
    kill "$_wd" 2>/dev/null
    wait "$_wd" 2>/dev/null
    command -p rm -f "$_work/live" "$_work/pid" "$_work/ps" "$_work/ready"
    command -p rmdir "$_work" 2>/dev/null

    [ "$_rc" -eq 0 ]
}

_root=${CLAUDE_PROJECT_DIR:-}
if [ -z "$_root" ]; then
    _root=${GROK_WORKSPACE_ROOT:-}
fi
if [ -z "$_root" ]; then
    _root=$(pwd)
fi

_seen=
_chosen=
_consider() {
    _c=$1
    [ -n "$_c" ] || return 0
    [ -z "$_chosen" ] || return 0
    case "$_seen" in
        *"|$_c|"*) return 0 ;;
    esac
    _seen="${_seen}|${_c}|"
    if _probe_viable "$_c"; then
        _chosen=$_c
    fi
}

_consider "${_root}/.venv/bin/python"
_consider /usr/bin/python3
_consider /opt/homebrew/bin/python3
_path_py=$(command -v python3 2>/dev/null) || _path_py=
_consider "$_path_py"

if [ -z "$_chosen" ]; then
    _fail_before_python "$@"
fi

# Isolated interpreter options precede the original argv (guard path first).
exec "$_chosen" -I "$@"
