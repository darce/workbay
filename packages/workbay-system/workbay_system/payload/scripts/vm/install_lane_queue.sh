#!/usr/bin/env bash
# Run once per VM by the operator or VM bootstrap. This installer is not wired
# into remote_agent.sh because it manages per-VM user systemd services.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
queue_source="$script_dir/lane_queue.py"
unit_names=(
    workbay-lane-queue.service
    workbay-lane-queue.path
    workbay-lane-queue.timer
)
config_home="${XDG_CONFIG_HOME:-$HOME/.config}"
queue_destination="$HOME/.local/lib/workbay/lane_queue.py"
unit_directory="$config_home/systemd/user"
dry_run=0
changed=0
temporary_file=''

usage() {
    printf 'Usage: %s [--dry-run]\n' "${0##*/}" >&2
}

if [ "$#" -gt 1 ]; then
    usage
    exit 2
fi
if [ "$#" -eq 1 ]; then
    if [ "$1" != '--dry-run' ]; then
        usage
        exit 2
    fi
    dry_run=1
fi

cleanup() {
    if [ -n "$temporary_file" ]; then
        rm -f "$temporary_file"
    fi
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

file_mode_matches() {
    local path="$1"
    local expected="$2"
    local actual

    if actual="$(stat -c '%a' "$path" 2>/dev/null)"; then
        :
    elif actual="$(stat -f '%Lp' "$path" 2>/dev/null)"; then
        :
    else
        return 1
    fi
    [ "$actual" = "$expected" ]
}

install_file() {
    local source="$1"
    local destination="$2"
    local mode="$3"
    local destination_directory

    if [ ! -f "$source" ]; then
        printf 'Missing installer source: %s\n' "$source" >&2
        return 1
    fi

    if [ -f "$destination" ] && [ ! -L "$destination" ] \
        && cmp -s "$source" "$destination" \
        && file_mode_matches "$destination" "$mode"; then
        return 0
    fi

    if [ "$dry_run" -eq 1 ]; then
        printf 'would install %s to %s (mode %s)\n' "$source" "$destination" "$mode"
        changed=1
        return 0
    fi

    destination_directory="${destination%/*}"
    mkdir -p "$destination_directory"
    temporary_file="$(mktemp "$destination_directory/.lane-queue-install.XXXXXX")"
    cp "$source" "$temporary_file"
    chmod "$mode" "$temporary_file"
    mv -f "$temporary_file" "$destination"
    temporary_file=''
    changed=1
    printf 'installed %s\n' "$destination"
}

systemd_units_ready() {
    local unit
    local enabled_state
    local active_state
    local ready=1

    for unit in workbay-lane-queue.path workbay-lane-queue.timer; do
        enabled_state=''
        active_state=''
        if ! enabled_state="$(timeout 30 systemctl --user is-enabled "$unit" 2>/dev/null)"; then
            enabled_state=''
        fi
        if ! active_state="$(timeout 30 systemctl --user is-active "$unit" 2>/dev/null)"; then
            active_state=''
        fi
        if [ "$enabled_state" != 'enabled' ] || [ "$active_state" != 'active' ]; then
            ready=0
        fi
    done

    [ "$ready" -eq 1 ]
}

install_file "$queue_source" "$queue_destination" 755
for unit_name in "${unit_names[@]}"; do
    install_file "$script_dir/$unit_name" "$unit_directory/$unit_name" 644
done

if [ "$dry_run" -eq 1 ]; then
    if [ "$changed" -eq 1 ] || ! systemd_units_ready; then
        printf 'would run: timeout 30 systemctl --user daemon-reload\n'
        printf 'would run: timeout 30 systemctl --user enable --now workbay-lane-queue.path workbay-lane-queue.timer\n'
    else
        printf 'unchanged\n'
    fi
    exit 0
fi

if [ "$changed" -eq 0 ] && systemd_units_ready; then
    printf 'unchanged\n'
    exit 0
fi

timeout 30 systemctl --user daemon-reload
timeout 30 systemctl --user enable --now workbay-lane-queue.path workbay-lane-queue.timer
