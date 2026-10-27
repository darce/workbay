#!/usr/bin/env bash
# Probe the gate host's installed Playwright headless Chromium capability.
set -euo pipefail

SCHEMA='workbay.ui_test_capability.v1'
MARKER='WORKBAY_UI_TEST_CAPABILITY_FIXTURE'

usage() {
    cat >&2 <<'EOF'
Usage:
  scripts/ui_test_capability.sh probe [--require] [--timeout SECONDS]
  scripts/ui_test_capability.sh install-plan <x.y.z>...
EOF
}

_json_quote() {
    local value=$1 escaped='' char hex code i
    for ((i = 0; i < ${#value}; i++)); do
        char=${value:i:1}
        case "$char" in
            '"') escaped+='\"' ;;
            '\') escaped+='\\' ;;
            $'\b') escaped+='\b' ;;
            $'\f') escaped+='\f' ;;
            $'\n') escaped+='\n' ;;
            $'\r') escaped+='\r' ;;
            $'\t') escaped+='\t' ;;
            *)
                if [[ "$char" == [[:cntrl:]] ]]; then
                    printf -v code '%d' "'$char"
                    printf -v hex '%04x' "$code"
                    escaped+="\\u${hex}"
                else
                    escaped+="$char"
                fi
                ;;
        esac
    done
    printf '"%s"' "$escaped"
}

_emit_probe() {
    local status=$1 browser=$2 browsers_path=$3 revisions_json=$4 node_json=$5
    local loopback=$6 launch_rc=$7 detail=$8 browser_json browsers_path_json detail_json
    if [ -n "$browser" ]; then
        browser_json=$(_json_quote "$browser")
    else
        browser_json=null
    fi
    browsers_path_json=$(_json_quote "$browsers_path")
    detail_json=$(_json_quote "$detail")
    printf '{"schema":"%s","status":"%s","browser":%s,"browsers_path":%s,"revisions":%s,"node":%s,"loopback_bind":%s,"launch_rc":%s,"detail":%s}\n' \
        "$SCHEMA" "$status" "$browser_json" "$browsers_path_json" "$revisions_json" \
        "$node_json" "$loopback" "$launch_rc" "$detail_json"
}

_revisions_json() {
    local revision first=1
    printf '['
    for revision in "$@"; do
        if [ "$first" -eq 0 ]; then printf ','; fi
        first=0
        _json_quote "$revision"
    done
    printf ']'
}

_highest_revision() {
    local candidate=$1 current=$2 candidate_number current_number
    candidate_number=${candidate#chromium_headless_shell-}
    current_number=${current#chromium_headless_shell-}
    case "$candidate_number:$current_number" in
        *[!0-9:]*|:*) return 1 ;;
    esac
    while [ "${candidate_number#0}" != "$candidate_number" ] && [ "${#candidate_number}" -gt 1 ]; do
        candidate_number=${candidate_number#0}
    done
    while [ "${current_number#0}" != "$current_number" ] && [ "${#current_number}" -gt 1 ]; do
        current_number=${current_number#0}
    done
    if [ "${#candidate_number}" -gt "${#current_number}" ]; then return 0; fi
    if [ "${#candidate_number}" -lt "${#current_number}" ]; then return 1; fi
    [[ "$candidate_number" > "$current_number" ]]
}

_find_browser() {
    local revision_dir=$1 candidate
    candidate=$(find "$revision_dir" -type f \( -name headless_shell -o -name chrome-headless-shell \) -print 2>/dev/null | sed -n '1p' || true)
    [ -n "$candidate" ] && [ -x "$candidate" ] || return 1
    printf '%s' "$candidate"
}

_loopback_bind() {
    if command -v node >/dev/null 2>&1; then
        node -e 'const net = require("net"); const server = net.createServer(); server.once("error", () => process.exit(1)); server.listen(0, "127.0.0.1", () => server.close(() => process.exit(0)));' >/dev/null 2>&1
        return $?
    fi
    if command -v python3 >/dev/null 2>&1; then
        python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); s.close()' >/dev/null 2>&1
        return $?
    fi
    return 1
}

_process_children() {
    ps -e -o pid= -o ppid= 2>/dev/null | awk -v parent="$1" '$2 == parent { print $1 }'
}

_signal_process_tree() {
    local parent=$1 signal_name=$2 child
    for child in $(_process_children "$parent"); do
        _signal_process_tree "$child" "$signal_name"
    done
    kill -"$signal_name" "$parent" 2>/dev/null || true
}

_cleanup_probe() {
    local child
    if [ -n "${browser_pid:-}" ]; then
        _signal_process_tree "$browser_pid" TERM
        sleep 1
        _signal_process_tree "$browser_pid" KILL
        wait "$browser_pid" 2>/dev/null || true
    fi
    if [ -n "${probe_tmp:-}" ]; then rm -rf "$probe_tmp"; fi
}

_probe() {
    local require=$1 timeout_seconds=$2
    local browsers_path browser='' best_revision='' best_number='' revision_dir revision_name
    local revisions=() node_json=null loopback=false status detail launch_rc=null
    local probe_tmp fixture stdout_file stderr_file browser_pid='' start_seconds timed_out=0
    local last_stderr candidate current_number node_version revisions_json

    if [ "${PLAYWRIGHT_BROWSERS_PATH+x}" = x ]; then
        browsers_path=$PLAYWRIGHT_BROWSERS_PATH
    else
        browsers_path="${HOME:-}/.cache/ms-playwright"
    fi

    if command -v node >/dev/null 2>&1; then
        node_version=$(node --version 2>/dev/null || true)
        if [ -n "$node_version" ]; then node_json=$(_json_quote "$node_version"); fi
    fi
    if _loopback_bind; then loopback=true; fi

    for revision_dir in "$browsers_path"/chromium_headless_shell-*; do
        [ -d "$revision_dir" ] || continue
        revision_name=${revision_dir##*/}
        revisions+=("$revision_name")
        candidate=$(_find_browser "$revision_dir" || true)
        [ -n "$candidate" ] || continue
        current_number=${revision_name#chromium_headless_shell-}
        case "$current_number" in ''|*[!0-9]*) continue ;; esac
        if [ -z "$best_revision" ] || _highest_revision "$revision_name" "$best_revision"; then
            best_revision=$revision_name
            browser=$candidate
        fi
    done

    revisions_json=$(_revisions_json "${revisions[@]+"${revisions[@]}"}")
    if [ -z "$browser" ]; then
        status=ui_test_capability_missing
        detail='no installed chromium headless shell found'
    else
        probe_tmp=$(mktemp -d "${TMPDIR:-/tmp}/workbay-ui-test.XXXXXX") || {
            status=ui_test_browser_launch_failed
            detail='could not create temporary probe directory'
            _emit_probe "$status" "$browser" "$browsers_path" "$revisions_json" "$node_json" "$loopback" "$launch_rc" "$detail"
            [ "$require" -eq 0 ] || return 69
            return 0
        }
        trap _cleanup_probe EXIT HUP INT TERM
        fixture="$probe_tmp/fixture.html"
        stdout_file="$probe_tmp/stdout"
        stderr_file="$probe_tmp/stderr"
        mkdir "$probe_tmp/profile"
        printf '<!doctype html><html><body>%s</body></html>\n' "$MARKER" > "$fixture"
        "$browser" --headless --no-sandbox --disable-gpu \
            "--user-data-dir=$probe_tmp/profile" --dump-dom "file://$fixture" \
            > "$stdout_file" 2> "$stderr_file" &
        browser_pid=$!
        start_seconds=$SECONDS
        while kill -0 "$browser_pid" 2>/dev/null; do
            if [ "$((SECONDS - start_seconds))" -ge "$timeout_seconds" ]; then
                timed_out=1
                _signal_process_tree "$browser_pid" TERM
                sleep 1
                _signal_process_tree "$browser_pid" KILL
                break
            fi
            sleep 1
        done
        if [ "$timed_out" -eq 1 ]; then
            wait "$browser_pid" 2>/dev/null || true
            launch_rc=124
        elif wait "$browser_pid"; then
            launch_rc=0
        else
            launch_rc=$?
        fi
        browser_pid=''
        last_stderr=$(tail -n 1 "$stderr_file" 2>/dev/null || true)
        if [ "$timed_out" -eq 1 ]; then
            status=ui_test_browser_launch_failed
            detail="browser launch timed out after ${timeout_seconds}s"
        elif [ "$launch_rc" -ne 0 ]; then
            status=ui_test_browser_launch_failed
            detail="browser exited with status ${launch_rc}"
        elif ! grep -Fq "$MARKER" "$stdout_file"; then
            status=ui_test_browser_launch_failed
            detail='browser output did not contain the fixture marker'
        else
            status=ok
            detail='headless Chromium launched and rendered the probe fixture'
        fi
        if [ -n "$last_stderr" ] && [ "$status" != ok ]; then
            detail="$detail: $last_stderr"
        fi
        _cleanup_probe
        trap - EXIT HUP INT TERM
    fi

    _emit_probe "$status" "$browser" "$browsers_path" "$revisions_json" "$node_json" "$loopback" "$launch_rc" "$detail"
    if [ "$require" -eq 1 ] && [ "$status" != ok ]; then return 69; fi
    return 0
}

_valid_version() {
    local version=$1 major minor patch
    case "$version" in *[!0-9.]*) return 1 ;; esac
    case "$version" in *.*.*) ;; *) return 1 ;; esac
    major=${version%%.*}
    version=${version#*.}
    minor=${version%%.*}
    patch=${version#*.}
    case "$patch" in *.*|"") return 1 ;; esac
    [ -n "$major" ] && [ -n "$minor" ] && [ -n "$patch" ]
}

cmd=${1:-}
[ "$#" -gt 0 ] && shift
case "$cmd" in
    probe)
        require=0
        timeout_seconds=30
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --require) require=1; shift ;;
                --timeout)
                    [ "$#" -ge 2 ] || { usage; exit 2; }
                    timeout_seconds=$2
                    case "$timeout_seconds" in ''|*[!0-9]*|0) usage; exit 2 ;; esac
                    shift 2
                    ;;
                *) usage; exit 2 ;;
            esac
        done
        _probe "$require" "$timeout_seconds"
        ;;
    install-plan)
        [ "$#" -gt 0 ] || { usage; exit 2; }
        first_version=$1
        for version in "$@"; do
            _valid_version "$version" || { echo "install-plan: invalid Playwright version '$version' (expected x.y.z)" >&2; exit 2; }
        done
        printf 'npx -y playwright@%s install-deps chromium  # needs root\n' "$first_version"
        for version in "$@"; do
            printf 'npx -y playwright@%s install chromium  # run as gate user\n' "$version"
        done
        ;;
    *) usage; exit 2 ;;
esac
