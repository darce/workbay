#!/usr/bin/env bash
# Remote test-gate offload (isolation-hardened; see docs/runbooks/remote-gate-provisioning.md).
#
# Pushes the current HEAD to a dedicated companion clone on a remote host and
# runs the requested make gate targets there as an UNPRIVILEGED, resource-capped
# user. Output contract for log consumers: one `=== <target> ===` header, then
# `EXIT=<code> (<target>)` per target, then a final `DONE-ALL`; local exit is
# nonzero if any target failed, 75 if the clone lock is busy, 74 if host-memory
# admission deferred the run. Only committed state is gated: HEAD is what runs
# remotely.
#
# Usage:
#   scripts/remote_gate.sh bootstrap            # one-time clone provisioning
#   scripts/remote_gate.sh doctor               # read-only host readiness probe
#   scripts/remote_gate.sh run [target ...]     # push HEAD + run gate targets
#
# Config — env > file (no baked-in host; unset host is a hard error):
#   0. HOST is REQUIRED — set it (no default; see resolution below). Other
#      knobs default (dir src/<repo-slug>, workdir ., targets below).
#   2. .workbay/remote-gate.env at the repo root: REMOTE_GATE_HOST,
#      REMOTE_GATE_DIR, REMOTE_GATE_WORKDIR, REMOTE_GATE_TARGETS,
#      REMOTE_GATE_ENV ("KEY=VALUE ..." injected into the remote run env)
#   3. WORKBAY_REMOTE_GATE_{HOST,DIR,WORKDIR,TARGETS,ENV,NICE,MEMORY_MAX,
#      CPU_QUOTA} + PYTEST_WORKERS env vars (captured before the file is
#      sourced, so env always wins)
#
# The remote user is expected to be a no-sudo, no-service-groups account whose
# user slice is capped (Phase 1 of the runbook); each run additionally wraps
# itself in a systemd-run scope with its own MemoryMax/CPUQuota.
# shellcheck disable=SC2016,SC1003  # single-quoted remote command strings:
# non-expansion is deliberate — those $vars must expand on the REMOTE side.
set -euo pipefail

# Resolve the MAIN checkout root (parent of the git-common-dir), not the linked
# worktree's toplevel — the config file and clone slug must be identical whether
# invoked from the main checkout or a linked session worktree, and `.workbay/`
# is gitignored so it only ever exists in the main checkout.
repo_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
repo_slug="$(basename "$repo_root")"

# Layer 3 capture FIRST: the config file is dot-sourced, so caller env must be
# snapshotted before sourcing or a file that sets caller-namespace vars would
# invert the documented env-over-file precedence.
_env_host="${WORKBAY_REMOTE_GATE_HOST:-}"
_env_dir="${WORKBAY_REMOTE_GATE_DIR:-}"
_env_workdir="${WORKBAY_REMOTE_GATE_WORKDIR:-}"
_env_targets="${WORKBAY_REMOTE_GATE_TARGETS:-}"
_env_extra="${WORKBAY_REMOTE_GATE_ENV:-}"
_env_nice="${WORKBAY_REMOTE_GATE_NICE:-}"
_env_workers="${PYTEST_WORKERS:-}"
_env_memmax="${WORKBAY_REMOTE_GATE_MEMORY_MAX:-}"
_env_cpuquota="${WORKBAY_REMOTE_GATE_CPU_QUOTA:-}"

# Layer 2: per-repo config file.
REMOTE_GATE_HOST="" REMOTE_GATE_DIR="" REMOTE_GATE_WORKDIR="" REMOTE_GATE_TARGETS="" REMOTE_GATE_ENV=""
config_file="$repo_root/.workbay/remote-gate.env"
if [ -f "$config_file" ]; then
    # shellcheck disable=SC1090
    . "$config_file"
fi

# Resolution: captured env > file. There is deliberately NO baked-in default
# host — a private tailnet address must never ship inside a would-be-distributed
# tool, and a fallback host is fail-open (an unconfigured caller would push HEAD
# to whatever address the default named). Unset => hard error, no fallback.
REMOTE_HOST="${_env_host:-${REMOTE_GATE_HOST:-}}"
if [ -z "$REMOTE_HOST" ]; then
    echo "remote-gate: host not configured — set WORKBAY_REMOTE_GATE_HOST or" \
         "REMOTE_GATE_HOST in .workbay/remote-gate.env (e.g. gate@<your-host>)" >&2
    exit 78
fi
REMOTE_DIR="${_env_dir:-${REMOTE_GATE_DIR:-src/${repo_slug}}}"
WORKDIR="${_env_workdir:-${REMOTE_GATE_WORKDIR:-.}}"
GATE_TARGETS="${_env_targets:-${REMOTE_GATE_TARGETS:-}}"
GATE_EXTRA_ENV="${_env_extra:-${REMOTE_GATE_ENV:-}}"
NICENESS="${_env_nice:-10}"
WORKERS="${_env_workers:-3}"
DEFAULT_TARGETS=(check-protocol check-system test-handoff)
# Bootstrap stamps this sentinel; run refuses without it so checkout/clean can
# never fire in a directory this script does not own.
CLONE_SENTINEL=".remote-gate-clone"
# Per-run scope caps (inside the outer user-slice fence from the runbook).
RUN_MEMORY_MAX="${_env_memmax:-6G}"
RUN_CPU_QUOTA="${_env_cpuquota:-200%}"

die() { echo "remote_gate: $*" >&2; exit 2; }

# --- validation (everything below is interpolated into a remote shell) -------
case "$REMOTE_DIR" in
    ""|.|/*|*..*) die "invalid REMOTE_DIR '${REMOTE_DIR}' (empty/./absolute/.. rejected)" ;;
    *"$repo_slug") : ;;
    *) die "REMOTE_DIR '${REMOTE_DIR}' must end with the repo slug '${repo_slug}' (collision guard)" ;;
esac
case "$WORKDIR" in
    /*|*..*) die "invalid REMOTE_GATE_WORKDIR '${WORKDIR}' (absolute/.. rejected)" ;;
esac
case "$REMOTE_DIR" in
    *[!A-Za-z0-9/_.-]*) die "REMOTE_DIR contains characters outside [A-Za-z0-9/_.-]" ;;
esac
case "$WORKDIR" in
    *[!A-Za-z0-9/_.-]*) die "REMOTE_GATE_WORKDIR contains characters outside [A-Za-z0-9/_.-]" ;;
esac
case "$NICENESS" in *[!0-9]*|"") die "WORKBAY_REMOTE_GATE_NICE must be an integer" ;; esac
case "$WORKERS" in *[!0-9]*|"") die "PYTEST_WORKERS must be an integer" ;; esac
case "$RUN_MEMORY_MAX" in *[!0-9GMK]*|"") die "MEMORY_MAX must look like 6G/512M" ;; esac
case "$RUN_CPU_QUOTA" in *[!0-9%]*|"") die "CPU_QUOTA must look like 200%" ;; esac

extra_env=()
# Word-split GATE_EXTRA_ENV without pathname expansion — an env value like
# `PATTERN=*` must stay literal, not glob against the cwd (shellcheck does not
# flag this). `set -f` is restored immediately after the split.
set -f
# deliberate unquoted split under noglob (tokens re-validated below)
# shellcheck disable=SC2206
_gate_env_tokens=($GATE_EXTRA_ENV)
set +f
for kv in ${_gate_env_tokens[@]+"${_gate_env_tokens[@]}"}; do
    case "$kv" in
    [A-Za-z_]*=*)
        case "$kv" in
        *'`'*|*'$('*|*';'*|*'&'*|*'|'*|*'<'*|*'>'*|*'\'*)
            die "REMOTE_GATE_ENV entry contains shell metacharacters: ${kv}" ;;
        esac
        extra_env+=("$kv")
        ;;
    *) die "REMOTE_GATE_ENV entries must be KEY=VALUE (got '${kv}')" ;;
    esac
done

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=10
     -o ServerAliveInterval=30 -o ServerAliveCountMax=4 "$REMOTE_HOST")

cmd="${1:-run}"
[ "$#" -gt 0 ] && shift

case "$cmd" in
bootstrap)
    # uv is provisioned by the runbook's Phase 1 (pinned tarball, no curl|sh);
    # bootstrap only prepares the receive clone.
    "${SSH[@]}" 'set -e
        command -v git >/dev/null || { echo "bootstrap: git missing on host" >&2; exit 1; }
        [ -x "$HOME/.local/bin/uv" ] || { echo "bootstrap: uv missing — run runbook Phase 1 first" >&2; exit 1; }
        mkdir -p "$HOME"/'"$REMOTE_DIR"'
        cd "$HOME"/'"$REMOTE_DIR"' || exit 1
        [ "$PWD" != "$HOME" ] || { echo "bootstrap: refusing to init \$HOME as the clone" >&2; exit 1; }
        [ -d .git ] || git init -q
        git config receive.denyCurrentBranch ignore
        # Test suites create fixture repos and commit in them; a fresh gate
        # user has no git identity and every such commit would fail (DBG-05).
        git config --global user.email >/dev/null 2>&1 || git config --global user.email "gate@remote-gate.invalid"
        git config --global user.name  >/dev/null 2>&1 || git config --global user.name  "remote-gate"
        touch '"$CLONE_SENTINEL"'
        echo "bootstrap: ok — $(git --version), clone at $PWD"'
    ;;
doctor)
    probe_urls=()
    for kv in ${extra_env[@]+"${extra_env[@]}"}; do
        case "$kv" in *://*) probe_urls+=("${kv#*=}") ;; esac
    done
    "${SSH[@]}" 'set -e
        echo "user=$(id -un) groups=$(id -Gn)"
        echo "arch=$(uname -m) cores=$(nproc)"
        free -h | awk "NR==2{print \"mem_available=\" \$7}"
        df -h "$HOME" | awk "NR==2{print \"disk_free=\" \$4}"
        du -sh "$HOME/.cache/uv" 2>/dev/null || echo "uv_cache=none"
        if [ -x "$HOME/.local/bin/uv" ]; then "$HOME/.local/bin/uv" --version; else echo "uv: MISSING (runbook Phase 1)"; fi
        command -v make >/dev/null && make --version | head -1 || echo "make: MISSING (apt-get install make)"
        command -v systemd-run >/dev/null || echo "systemd-run: MISSING (per-run caps unavailable)"
        hostgov_found=""
        for cand in "$HOME/'"$REMOTE_DIR"'/'"$WORKDIR"'/.venv/bin/workbay-hostgov" "$HOME/.local/bin/workbay-hostgov" "$(command -v workbay-hostgov 2>/dev/null || true)"; do
            [ -n "$cand" ] && [ -x "$cand" ] && { hostgov_found="$cand"; break; }
        done
        if [ -n "$hostgov_found" ]; then
            echo "workbay-hostgov: present at $hostgov_found (memory admission active)"
        else
            echo "workbay-hostgov: MISSING (admission hook is a no-op; merge internal so uv sync installs it into the clone .venv)"
        fi
        if [ -f "$HOME"/'"$REMOTE_DIR"'/'"$CLONE_SENTINEL"' ]; then echo "clone: present"; else echo "clone: MISSING (run bootstrap)"; fi
        for url in '"${probe_urls[*]:-}"'; do
            hostport="${url#*://}"; hostport="${hostport#*@}"; hostport="${hostport%%/*}"
            host="${hostport%%:*}"; port="${hostport##*:}"
            case "$port" in ""|*[!0-9]*)
                echo "service ${host}: no explicit port in DSN — reachability probe skipped"
                continue ;;
            esac
            if nc -z -w3 "$host" "$port" 2>/dev/null; then echo "service ${host}:${port}: reachable"
            else echo "service ${host}:${port}: UNREACHABLE (tests depending on it may silently skip)"; fi
        done'
    ;;
run)
    targets=("$@")
    if [ "${#targets[@]}" -eq 0 ] && [ -n "$GATE_TARGETS" ]; then
        read -r -a targets <<< "$GATE_TARGETS"
    fi
    [ "${#targets[@]}" -gt 0 ] || targets=("${DEFAULT_TARGETS[@]}")
    for t in "${targets[@]}"; do
        case "$t" in
        *[!A-Za-z0-9_.-]*) die "refusing target with unsafe characters: ${t}" ;;
        esac
    done
    dirty="$(git status --porcelain | wc -l | tr -d ' ')"
    if [ "$dirty" -gt 0 ]; then
        echo "remote_gate: WARNING — ${dirty} dirty path(s) NOT gated (only committed HEAD is pushed)" >&2
    fi
    sha="$(git rev-parse HEAD)"
    echo "remote-gate: pushing ${sha} to ${REMOTE_HOST}:${REMOTE_DIR} (workdir ${WORKDIR})"
    git push --quiet --force "${REMOTE_HOST}:${REMOTE_DIR}" "HEAD:refs/heads/remote-gate"
    "${SSH[@]}" "set -u
        cd \"\$HOME/${REMOTE_DIR}\" || exit 1
        [ -f \"${CLONE_SENTINEL}\" ] || { echo 'remote-gate: clone sentinel missing; refusing (re-run bootstrap)' >&2; exit 1; }
        exec 9>.gate.lock
        flock -n 9 || { echo 'remote-gate: gate busy (another run holds the clone lock)' >&2; exit 75; }
        git checkout -qf ${sha} || exit 1
        git clean -fdq -e .venv -e .venv-handoff -e ${CLONE_SENTINEL} -e .gate.lock || exit 1
        cd \"${WORKDIR}\" || exit 1
        # docs/workbay/{rules,contracts} are install-materialized mirrors of
        # the payload canon (implementation note; rules fully gitignored, contracts
        # partially tracked), so a bare clone never carries the full trees and
        # the overlay drift gate could never pass remotely. Re-derive both from
        # the just-checked-out payload every run (git clean wiped the previous
        # untracked copies), matching what dogfood does on a self-host root.
        for surf in rules contracts; do
            canon=\"packages/workbay-system/workbay_system/payload/docs/workbay/\$surf\"
            if [ -d \"\$canon\" ]; then
                rm -rf \"docs/workbay/\$surf\"
                mkdir -p docs/workbay
                cp -R \"\$canon\" \"docs/workbay/\$surf\" || exit 1
            fi
        done
        if [ -f pyproject.toml ]; then
            \"\$HOME/.local/bin/uv\" sync -q || { echo 'remote-gate: uv sync failed' >&2; exit 1; }
        else
            echo 'remote-gate: no pyproject.toml in workdir — skipping uv sync'
        fi
        # Admission gate (internal): refuse to start under memory pressure —
        # a deferred run is recoverable; an OOM-killed co-resident service is
        # not. Runs AFTER uv sync so the console script the sync installs into
        # \$PWD/.venv/bin is visible; the hook is a no-op only when the CLI is
        # genuinely absent (logged, so a skip is never silent). Exit 74 (defer,
        # distinct from the lock-busy 75) so automation can tell them apart.
        hostgov=''
        for cand in \"\$PWD/.venv/bin/workbay-hostgov\" \"\$HOME/.local/bin/workbay-hostgov\"; do
            [ -x \"\$cand\" ] && { hostgov=\"\$cand\"; break; }
        done
        [ -n \"\$hostgov\" ] || hostgov=\"\$(command -v workbay-hostgov 2>/dev/null || true)\"
        if [ -n \"\$hostgov\" ]; then
            \"\$hostgov\" probe --json --workspace-root \"\$PWD\" \
                || { echo 'remote-gate: DEFERRED — host memory admission (workbay-hostgov); retryable' >&2; exit 74; }
        else
            echo 'remote-gate: workbay-hostgov not installed — memory admission SKIPPED (systemd caps remain the backstop)' >&2
        fi
        runner='nice -n ${NICENESS} ionice -c3'
        if command -v systemd-run >/dev/null && systemd-run --quiet --user --scope -p MemoryMax=${RUN_MEMORY_MAX} true 2>/dev/null; then
            runner=\"systemd-run --quiet --user --scope -p MemoryMax=${RUN_MEMORY_MAX} -p CPUQuota=${RUN_CPU_QUOTA} nice -n ${NICENESS} ionice -c3\"
        else
            echo 'remote-gate: systemd-run scope unavailable — falling back to nice/ionice only' >&2
        fi
        overall=0
        for t in ${targets[*]}; do
            echo \"=== \$t ===\"
            \$runner env \
                ${extra_env[*]:-} \
                TMPDIR=/tmp \
                WORKBAY_DISABLE_INVOKING_HOOKS=1 \
                WORKBAY_HANDOFF_DEFAULT_AGENT=\${WORKBAY_HANDOFF_DEFAULT_AGENT:-remote-gate} \
                PYTEST_WORKERS=${WORKERS} \
                PATH=\"\$PWD/.venv/bin:\$HOME/.local/bin:\$PATH\" \
                make \"\$t\"
            rc=\$?
            echo \"EXIT=\$rc (\$t)\"
            [ \"\$rc\" -eq 0 ] || overall=1
        done
        echo DONE-ALL
        exit \$overall"
    ;;
*)
    sed -n '2,27p' "$0" >&2
    exit 2
    ;;
esac
