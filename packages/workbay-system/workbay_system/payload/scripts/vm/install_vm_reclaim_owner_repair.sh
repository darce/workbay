#!/usr/bin/env bash
# One-time operator install: sudo bash scripts/vm/install_vm_reclaim_owner_repair.sh
set -euo pipefail
if [ "$(id -u)" -ne 0 ] || [ "$#" -ne 0 ]; then
    printf 'Run as root with no arguments.\n' >&2
    exit 2
fi
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
changed=0
temporary_file=''
trap 'if [ -n "$temporary_file" ]; then rm -f "$temporary_file"; fi' EXIT
install_file() {
    local source="$1" destination="$2" mode="$3"
    if [ ! -L "$destination" ] && [ -f "$destination" ] \
        && cmp -s "$source" "$destination" \
        && [ "$(stat -c '%a' "$destination")" = "$mode" ] \
        && [ "$(stat -c '%u:%g' "$destination")" = '0:0' ]; then
        return
    fi
    temporary_file="$(mktemp "${destination%/*}/.owner-repair-install.XXXXXX")"
    install -m "$mode" "$source" "$temporary_file"
    chown root:root "$temporary_file"
    mv -fT "$temporary_file" "$destination"
    temporary_file=''
    changed=1
}
# These ancestors are operator-owned, never inside gate's writable home.
for directory in /usr/local/lib/workbay /etc/systemd/system; do
    if [ -L "$directory" ]; then
        printf 'Refusing symlink install directory: %s\n' "$directory" >&2
        exit 2
    fi
    install -d -m 755 "$directory"
    chown root:root "$directory"
done
install_file "$script_dir/workbay_vm_reclaim_owner_repair.py" /usr/local/lib/workbay/workbay_vm_reclaim_owner_repair.py 755
for unit in workbay-vm-reclaim-owner-repair.service workbay-vm-reclaim-owner-repair.path; do
    install_file "$script_dir/$unit" "/etc/systemd/system/$unit" 644
done
# Reload even when files already match: a prior run may have stopped after
# publication and before systemd read them.
timeout 30 systemctl daemon-reload
timeout 30 systemctl enable --now workbay-vm-reclaim-owner-repair.path
if [ "$changed" -eq 0 ]; then printf 'unchanged\n'; fi
