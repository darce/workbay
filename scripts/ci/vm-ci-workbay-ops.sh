#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: vm-ci-workbay-ops.sh <command> [options]

  help | --help
  check-site
  status [--dry-run]
  publish-dry-run [--commit SHA] [--remote [--remote-dir DIR] [--dry-run]]
  publish [--commit SHA] [--remote [--remote-dir DIR] [--dry-run]]
                                         Requires CONFIRM=PUBLISH
  tmp-clean [--dry-run] [--confirm CLEAN] [--age-days N]
                                         Defaults to age 7 days

SSH host precedence: VMCI_HOST, OCI_HOST, .workbay/remote-gate.env.
Remote checkout: --remote-dir, REMOTE_GATE_DIR in .workbay/remote-gate.env,
or src/workbay.
With --remote, --dry-run prints the SSH command without connecting.
Set VMCI_SITE_CONFIG to override config/vm-ci/site.json.
EOF
}

die() {
    printf 'vm-ci-workbay-ops: %s\n' "$*" >&2
    exit 2
}

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$repo_root"

gate_setting() {
    local config=.workbay/remote-gate.env line value setting=
    local assignment="^[[:blank:]]*(export[[:blank:]]+)?$1=(.*)$"
    if [[ ! -f $config ]]; then
        local common_dir
        common_dir=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null) || return 0
        config="$(dirname -- "$common_dir")/$config"
        [[ -f $config ]] || return 0
    fi

    # Read literal assignments, including the quoted form emitted by WorkBay.
    while IFS= read -r line || [[ -n $line ]]; do
        if [[ ${line%$'\r'} =~ $assignment ]]; then
            value=${BASH_REMATCH[2]}
            case $value in
                \"*\") value=${value:1:${#value}-2} ;;
                \'*\') value=${value:1:${#value}-2} ;;
            esac
            setting=$value
        fi
    done < "$config"
    printf '%s' "$setting"
}

resolve_host() {
    local LC_ALL=C gate
    VMCI_HOST=${VMCI_HOST:-${OCI_HOST:-}}
    if [[ -z $VMCI_HOST ]]; then
        gate=$(gate_setting REMOTE_GATE_HOST)
        VMCI_HOST=${gate#*@}
        if [[ $gate == *@* ]]; then
            [[ -n ${gate%%@*} ]] || die 'invalid user in REMOTE_GATE_HOST'
            VMCI_USER=${VMCI_USER-${gate%%@*}}
        fi
    fi

    [[ $VMCI_HOST =~ ^[A-Za-z0-9_.:][A-Za-z0-9_.:-]*$ ]] ||
        die 'invalid SSH host (set VMCI_HOST, OCI_HOST, or REMOTE_GATE_HOST)'
    if [[ -n ${VMCI_USER:-} ]]; then
        [[ $VMCI_USER =~ ^[A-Za-z0-9_.][A-Za-z0-9_.-]*$ ]] || die 'invalid SSH user (VMCI_USER)'
    fi
    export VMCI_HOST VMCI_USER
}

site() {
    VMCI_SITE_CONFIG=${VMCI_SITE_CONFIG:-config/vm-ci/site.json} \
        python3 -I -B scripts/ci/vm_ci_site.py "$@"
}

status_script() {
    local config
    unset VMCI_SITE_CI_ROOT VMCI_SITE_STATE_DIR VMCI_SITE_POLL_TIMER VMCI_SITE_POLL_SERVICE VMCI_SITE_RUNNER_HOME
    config=$(site shell) || return
    eval "$config" || return
    : "${VMCI_SITE_CI_ROOT:?}" "${VMCI_SITE_STATE_DIR:?}" \
        "${VMCI_SITE_POLL_TIMER:?}" "${VMCI_SITE_POLL_SERVICE:?}"

    printf 'ci_root=%q\nstate_dir=%q\npoll_timer=%q\npoll_service=%q\n' \
        "$VMCI_SITE_CI_ROOT" "$VMCI_SITE_STATE_DIR" "$VMCI_SITE_POLL_TIMER" "$VMCI_SITE_POLL_SERVICE"
    cat <<'EOF'
rc=0
ls -ld -- "$ci_root/controller/current" || rc=1
git --git-dir="$ci_root/mirror.git" rev-parse --verify refs/heads/main || rc=1
df -h / || rc=1
systemctl --user is-active -- "$poll_timer" "$poll_service" || rc=1
cat -- "$state_dir/cursor.json" || rc=1
exit "$rc"
EOF
}

run_remote() {
    local dry_run=$1 script=$2
    local -a SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=15)
    local -a ssh_cmd=("${VMCI_SSH_BIN:-ssh}" "${SSH_OPTS[@]}" --
        "${VMCI_USER:+$VMCI_USER@}$VMCI_HOST" bash -s)
    if [[ $dry_run == 1 ]]; then
        printf '%q ' "${ssh_cmd[@]}"
        printf "<<'VMCI_REMOTE_SCRIPT'\n%s\nVMCI_REMOTE_SCRIPT\n" "$script"
    else
        "${ssh_cmd[@]}" <<< "$script"
    fi
}

command=${1:-help}
if (( $# )); then shift; fi

case $command in
    --help|help)
        [[ $# == 0 ]] || die 'help takes no arguments'
        usage
        ;;
    check-site)
        [[ $# == 0 ]] || die 'check-site takes no arguments'
        site check
        site json
        ;;
    status)
        dry_run=0
        if [[ ${1:-} == --dry-run ]]; then dry_run=1; shift; fi
        [[ $# == 0 ]] || die 'usage: status [--dry-run]'
        resolve_host
        script=$(status_script)
        run_remote "$dry_run" "$script"
        ;;
    publish-dry-run|publish)
        args=()
        remote=0 dry_run=0 remote_dir= commit=
        if [[ $command == publish ]]; then
            [[ ${CONFIRM:-} == PUBLISH ]] || die 'publish requires CONFIRM=PUBLISH'
            args+=(--execute)
        fi
        args+=(--json)
        while (( $# )); do
            case $1 in
                --remote) remote=1; shift ;;
                --dry-run) dry_run=1; shift ;;
                --remote-dir)
                    [[ -n ${2:-} && $2 != -* ]] || die '--remote-dir requires a directory'
                    remote_dir=$2; shift 2 ;;
                --commit)
                    [[ -n ${2:-} && $2 != -* ]] || die '--commit requires a SHA'
                    commit=$2; shift 2 ;;
                *) die "unknown $command option: $1" ;;
            esac
        done
        if [[ -n $commit ]]; then args+=(--commit "$commit"); fi
        if [[ $remote == 0 && ( $dry_run == 1 || -n $remote_dir ) ]]; then
            die '--dry-run and --remote-dir require --remote'
        fi
        resolve_host
        if [[ $remote == 1 ]]; then
            if [[ -z $remote_dir ]]; then remote_dir=$(gate_setting REMOTE_GATE_DIR); fi
            printf -v script 'CDPATH= cd -- %q || exit\n' "${remote_dir:-src/workbay}"
            if [[ $command == publish ]]; then script+='CONFIRM=PUBLISH '; fi
            printf -v publish_cmd ' %q' bash scripts/ci/vm-ci-workbay-publish.sh "${args[@]}"
            script+="exec$publish_cmd"
            run_remote "$dry_run" "$script"
            exit
        fi
        exec bash scripts/ci/vm-ci-workbay-publish.sh "${args[@]}"
        ;;
    tmp-clean)
        dry_run=0 confirm='' age_days=7
        while (( $# )); do
            case $1 in
                --dry-run) dry_run=1; shift ;;
                --confirm)
                    [[ ${2:-} == CLEAN ]] || die 'tmp-clean confirmation must be CLEAN'
                    confirm=$2; shift 2 ;;
                --age-days)
                    [[ ${2:-} =~ ^[1-9][0-9]*$ ]] || die '--age-days must be a positive integer'
                    age_days=$2; shift 2 ;;
                *) die 'usage: tmp-clean [--dry-run] [--confirm CLEAN] [--age-days N]' ;;
            esac
        done
        [[ $dry_run == 1 || $confirm == CLEAN ]] || die 'tmp-clean requires --dry-run or --confirm CLEAN'
        resolve_host
        # Limit cleanup to old VM-CI regular files owned by the SSH user.
        printf -v script "find -P /tmp -xdev -maxdepth 1 -type f -uid \"\$(id -u)\" -name 'vm-ci-*' -mtime +%s" "$age_days"
        if [[ $dry_run == 0 ]]; then script+=' -delete'; fi
        script+=' -print'
        run_remote "$dry_run" "$script"
        ;;
    *) die "unknown command: $command (see --help)" ;;
esac
