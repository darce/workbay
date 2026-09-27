#!/usr/bin/env bash
# CI status reporter — runs the remote gate and publishes one GitHub commit
# status per gate target, so the gate host serves the role GitHub Actions
# `test.yml` otherwise plays. No workflow file is touched: `test.yml` stays on
# disk (packages/workbay-system/tests/make/test_workbay_ci_gate.py pins it), and
# whether GitHub itself runs it is a repository setting, not this script's job.
#
# Usage:
#   scripts/ci_report.sh                       # CI-parity target set against HEAD
#   scripts/ci_report.sh --sha <sha>           # assert the commit being gated
#   scripts/ci_report.sh check-system          # subset of targets
#   scripts/ci_report.sh --dry-run             # print the status calls, post none
#   scripts/ci_report.sh --from-log <path>     # re-report a finished run; no gate
#
# `--dry-run` suppresses only the posting — it still runs the gate. To exercise
# the reporter without spending host time, pair it with `--from-log`.
#
# Contract, per target: one commit status with context `remote-gate/<target>`,
# state pending -> success|failure. Plus a rollup context `remote-gate` that is
# the AND of every target. Commit statuses are append-only on GitHub: a later
# run with the same context supersedes the earlier one in the combined view,
# but the earlier one is never deleted.
#
# The `gh` token lives on the gate host, never on this laptop, and is invoked by
# ABSOLUTE path: bash resolves a command name against the CURRENT PATH before
# applying a `VAR=value` assignment prefix, so `PATH=$HOME/.local/bin:$PATH gh`
# cannot find a gh that lives outside the non-interactive ssh PATH.
#
# Exit: 0 all green; 1 a target failed; 74/75 the gate deferred (host-memory
# admission / clone lock busy) — retryable, statuses are left pending; 2 usage
# or reporting error.
set -euo pipefail

# shellcheck disable=SC2016  # single-quoted: $HOME must expand on the REMOTE side
REMOTE_GH='$HOME/.local/bin/gh'

# owner/name is DERIVED from origin, never baked in. A literal slug would ship
# this repo's private path into any public export of scripts/, and would make
# the script wrong for every consumer repo that copies it.
origin_slug() {
    local url
    url="$(git config --get remote.origin.url 2>/dev/null || true)"
    [ -n "$url" ] || return 1
    url="${url%.git}"
    case "$url" in
        *:*/*)  printf '%s\n' "${url##*:}" ;;       # git@host:owner/name
        */*/*)  printf '%s\n' "${url#*://*/}" ;;    # https://host/owner/name
        *)      return 1 ;;
    esac
}

# CI-parity target set. Each entry mirrors a job in .github/workflows/test.yml;
# `scripts-tests` has no entry of its own because `check-system` already runs
# `scripts/tests`. `check-system-integration` has no test.yml counterpart — it
# is the integration half that `check-system` excludes, and gating it here is a
# deliberate superset of what CI covers.
CI_PARITY_TARGETS=(
    brand-check                 # job: brand-check
    format-check                # job: format
    check-protocol              # job: workbay-protocol
    check-bootstrap             # job: workbay-bootstrap
    test-handoff                # job: mcp-workbay-handoff
    test-orchestrator           # job: mcp-workbay-orchestrator
    test-bridge                 # job: workbay-codex-bridge
    check-system                # jobs: workbay-system, scripts-tests
    check-system-integration    # (superset; no test.yml counterpart)
    check-workbay               # job: workbay
    test-contract               # job: cross-package-contract
)

die() { echo "ci-report: $*" >&2; exit 2; }

# Two different roots, and conflating them is a silent wrong-commit gate.
#   worktree_root: the checkout whose HEAD remote_gate.sh will push. Run from a
#     linked worktree, that is the feature branch — cd'ing to the main checkout
#     instead would gate main and publish the verdict under the branch's name.
#   config_root: the MAIN checkout, where the gitignored `.workbay/` config and
#     the durable `.task-state/` log dir live (a worktree gets reaped).
worktree_root="$(git rev-parse --show-toplevel)"
config_root="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")"
cd "$worktree_root"

# --- args -------------------------------------------------------------------
SHA=""
REPO_SLUG="${WORKBAY_CI_REPORT_REPO:-}"
DRY_RUN=0
FROM_LOG=""
targets=()
while [ $# -gt 0 ]; do
    case "$1" in
        --sha)      [ $# -ge 2 ] || die "--sha needs a value"; SHA="$2"; shift 2 ;;
        --repo)     [ $# -ge 2 ] || die "--repo needs a value"; REPO_SLUG="$2"; shift 2 ;;
        --from-log) [ $# -ge 2 ] || die "--from-log needs a value"; FROM_LOG="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=1; shift ;;
        -h|--help)  sed -n '2,31p' "$0"; exit 0 ;;
        -*)         die "unknown flag '$1'" ;;
        *)          targets+=("$1"); shift ;;
    esac
done
[ ${#targets[@]} -gt 0 ] || targets=("${CI_PARITY_TARGETS[@]}")

[ -n "$REPO_SLUG" ] || REPO_SLUG="$(origin_slug)" \
    || die "cannot derive owner/name from remote.origin.url — pass --repo owner/name or set WORKBAY_CI_REPORT_REPO"
case "$REPO_SLUG" in
    */*/*) die "--repo must be owner/name, got '${REPO_SLUG}'" ;;
    */*) : ;;
    *) die "--repo must be owner/name, got '${REPO_SLUG}'" ;;
esac
case "$REPO_SLUG" in
    *[!A-Za-z0-9/_.-]*) die "--repo contains characters outside [A-Za-z0-9/_.-]" ;;
esac

# Same charset the gate itself enforces; every target is interpolated into both
# a remote shell and a status context.
for t in "${targets[@]}"; do
    case "$t" in
        ""|*[!A-Za-z0-9_.-]*) die "refusing target with unsafe characters: '${t}'" ;;
    esac
done

# --- commit under test ------------------------------------------------------
# remote_gate.sh pushes HEAD, so HEAD is what runs. `--sha` is an assertion,
# not a selector: a value that is not HEAD would attach this run's verdict to a
# commit it did not test.
head_sha="$(git rev-parse HEAD)"
if [ -n "$SHA" ]; then
    SHA="$(git rev-parse --verify "${SHA}^{commit}")" || die "unknown commit"
    [ "$SHA" = "$head_sha" ] || die "--sha ${SHA} is not HEAD (${head_sha}); the gate runs HEAD, so the status would not describe that commit"
fi
SHA="$head_sha"

# A status POST for a commit GitHub has never seen is a 422. Any origin ref
# counts — a feature branch is as valid a status target as main — so ask which
# remote branches contain it, not whether it is an ancestor of one in
# particular. Warn rather than refuse: the push may be in flight, and the gate
# run is the expensive part.
if [ -z "$(git branch -r --contains "$SHA" --list 'origin/*' 2>/dev/null)" ]; then
    echo "ci-report: WARNING — ${SHA} is on no origin branch; push it first or the status POST will 422." >&2
fi

# --- gate host --------------------------------------------------------------
# Same precedence as scripts/remote_gate.sh: captured env beats the config file.
_env_host="${WORKBAY_REMOTE_GATE_HOST:-}"
REMOTE_GATE_HOST=""
config_file="${config_root}/.workbay/remote-gate.env"
if [ -f "$config_file" ]; then
    # shellcheck disable=SC1090
    . "$config_file"
fi
GATE_HOST="${_env_host:-${REMOTE_GATE_HOST:-}}"
[ -n "$GATE_HOST" ] || die "gate host not configured — set WORKBAY_REMOTE_GATE_HOST or REMOTE_GATE_HOST in .workbay/remote-gate.env"
case "$GATE_HOST" in
    *[!A-Za-z0-9@/_.:-]*) die "gate host contains characters outside [A-Za-z0-9@/_.:-]" ;;
esac

# --- status posting ---------------------------------------------------------
# One ssh per status. gh reads its token from the gate host's own gh config;
# nothing secret crosses this laptop. state/context/description travel as
# positional args to a remote `bash -s` script rather than spliced into it.
post_status() {
    local state="$1" context="$2" description="$3"
    local remote_args
    # GitHub truncates descriptions past 140 chars; truncate here so the stored
    # value is what we intended rather than whatever GitHub happened to keep.
    description="$(printf '%.140s' "$description")"
    if [ "$DRY_RUN" -eq 1 ]; then
        printf 'DRY-RUN  %-8s %-34s %s\n' "$state" "$context" "$description"
        return 0
    fi
    printf -v remote_args '%q ' "$state" "$context" "$description"
    ssh -o BatchMode=yes "$GATE_HOST" \
        "bash -s ${remote_args}" <<REMOTE \
        || { echo "ci-report: status POST failed (${context}=${state})" >&2; return 1; }
set -euo pipefail
${REMOTE_GH} api -X POST repos/${REPO_SLUG}/statuses/${SHA} \
    -f state="\$1" -f context="\$2" -f description="\$3" --silent
REMOTE
}

post_failed=0

if [ -n "$FROM_LOG" ]; then
    # Re-report from a log a previous run already produced: no push, no gate,
    # no host time. This is how you recover from a status POST that failed
    # after the suites were green, and how the reporter itself is smoke-tested
    # (a --dry-run alone still runs the gate — it only suppresses posting).
    gate_log="$FROM_LOG"
    [ -r "$gate_log" ] || die "--from-log '${gate_log}' is not readable"
    gate_rc=0
    grep -q '^EXIT=[1-9]' "$gate_log" && gate_rc=1
    echo "ci-report: repo=${REPO_SLUG} sha=${SHA} host=${GATE_HOST}"
    echo "ci-report: re-reporting from ${gate_log} (no gate run)"
    echo "ci-report: targets: ${targets[*]}"
else
    log_dir="${config_root}/.task-state/ci-report/${SHA}"
    mkdir -p "$log_dir"
    gate_log="${log_dir}/gate.log"

    echo "ci-report: repo=${REPO_SLUG} sha=${SHA} host=${GATE_HOST}"
    echo "ci-report: targets: ${targets[*]}"
    echo "ci-report: log: ${gate_log}"

    for t in "${targets[@]}"; do
        post_status pending "remote-gate/${t}" "queued on the gate host" || post_failed=1
    done
    post_status pending "remote-gate" "gate running (${#targets[@]} targets)" || post_failed=1
    if [ "$post_failed" -ne 0 ]; then
        die "could not publish pending statuses — aborting before the expensive run"
    fi

    # --- run the gate -------------------------------------------------------
    # `set -e` is off for this one command: a nonzero gate exit is the signal
    # this script exists to report, not a reason to die before reporting it.
    set +e
    scripts/remote_gate.sh run "${targets[@]}" 2>&1 | tee "$gate_log"
    gate_rc="${PIPESTATUS[0]}"
    set -e
fi

# 75 = clone lock busy, 74 = host-memory admission deferred. Neither is a test
# verdict; leaving the per-target statuses pending is the honest report, and
# re-running is the remedy.
if [ "$gate_rc" -eq 75 ] || [ "$gate_rc" -eq 74 ]; then
    reason="clone lock busy"
    [ "$gate_rc" -eq 74 ] && reason="host-memory admission deferred"
    echo "ci-report: gate deferred (${reason}); statuses left pending — re-run when the host frees up." >&2
    post_status pending "remote-gate" "deferred: ${reason} — re-run" || true
    exit "$gate_rc"
fi

# --- parse + report ---------------------------------------------------------
# Output contract (scripts/remote_gate.sh): `=== <target> ===`, then
# `EXIT=<code> (<target>)` per target, then a final `DONE-ALL`. A target with no
# EXIT line never ran — the gate died before reaching it — which is `error`
# (something went wrong reporting), not `failure` (the suite is red).
failed=()
errored=()
passed=()
for t in "${targets[@]}"; do
    rc="$(sed -n "s/^EXIT=\([0-9][0-9]*\) (${t})\$/\1/p" "$gate_log" | tail -n 1)"
    if [ -z "$rc" ]; then
        errored+=("$t")
        post_status error "remote-gate/${t}" "did not run — gate exited ${gate_rc} before this target" || post_failed=1
    elif [ "$rc" -eq 0 ]; then
        passed+=("$t")
        post_status success "remote-gate/${t}" "passed on the gate host" || post_failed=1
    else
        failed+=("$t")
        post_status failure "remote-gate/${t}" "make ${t} exited ${rc}" || post_failed=1
    fi
done

if ! grep -q '^DONE-ALL$' "$gate_log"; then
    echo "ci-report: WARNING — no DONE-ALL in the gate log; the run was cut short." >&2
fi

n_fail=${#failed[@]}
n_err=${#errored[@]}
n_pass=${#passed[@]}
if [ "$n_fail" -eq 0 ] && [ "$n_err" -eq 0 ]; then
    post_status success "remote-gate" "${n_pass}/${#targets[@]} targets passed" || post_failed=1
    rollup=0
else
    post_status failure "remote-gate" "${n_pass} passed, ${n_fail} failed, ${n_err} did not run" || post_failed=1
    rollup=1
fi

echo
echo "ci-report: passed(${n_pass}): ${passed[*]-}"
[ "$n_fail" -gt 0 ] && echo "ci-report: FAILED(${n_fail}): ${failed[*]}"
[ "$n_err" -gt 0 ] && echo "ci-report: DID-NOT-RUN(${n_err}): ${errored[*]}"
echo "ci-report: statuses posted to https://github.com/${REPO_SLUG}/commit/${SHA}"

[ "$post_failed" -eq 0 ] || die "one or more status POSTs failed — the published result is incomplete"
exit "$rollup"
