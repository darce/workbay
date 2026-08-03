#!/usr/bin/env bash
#
# Release helper for the WorkBay repository (git-only delivery).
#
# Usage:
#   scripts/release.sh monorepo <vX.Y.Z>         # cut the consumer-facing monorepo tag
#   scripts/release.sh plan [vX.Y.Z] [--json]    # print the computed release plan
#   scripts/release.sh status                    # show git-tag release state for each package
#   scripts/release.sh preflight                 # run the pre-release checklist only
#
# Flags:
#   --dry-run       print what would be done, take no destructive action
#   --skip-tests    skip the per-package pytest run (still runs contract + rehearsal)
#   --auto-stash-dashboard   temporarily stash a dirty DASHBOARD.txt during release checks/tagging
#
# Delivery: git-only. PyPI publishing is retired; the per-package <pkg>-vX.Y.Z
# tags are pushed by the public mirror sync + tag-sync (scripts/release_public.py,
# `make release-public`). This helper cuts the consumer monorepo tag and reports
# git-tag state — it never builds, uploads, or queries PyPI.
#
# Authoritative playbook: docs/RELEASING.md.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ----- argument parsing ------------------------------------------------------

DRY_RUN=0
SKIP_TESTS=0
AUTO_STASH_DASHBOARD=0
JSON_OUTPUT=0
POSITIONAL=()

PACKAGES=()
WORKTREE_PREPARED=0
STASHED_DASHBOARD=0
STASH_REF=""
RUN_ID=""
RUN_LOG_DIR=""
LAST_FAILURE_LOG=""
PREFLIGHT_RESULTS_FILE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --skip-tests) SKIP_TESTS=1; shift ;;
        --auto-stash-dashboard) AUTO_STASH_DASHBOARD=1; shift ;;
        --json) JSON_OUTPUT=1; shift ;;
        -h|--help)
            sed -n '3,14p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done

set -- "${POSITIONAL[@]:-}"
SUBCOMMAND="${1:-}"

# ----- helpers ---------------------------------------------------------------

log()  { printf '\033[1;34m[release]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[release]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[release]\033[0m %s\n' "$*" >&2; exit 1; }

run() {
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '\033[1;90m[dry-run]\033[0m %s\n' "$*"
    else
        log "$*"
        eval "$@"
    fi
}

run_logged() {
    local label="$1" logfile="$2" command="$3"
    local elapsed status

    ensure_run_log_dir
    elapsed=0

    if [[ $DRY_RUN -eq 1 ]]; then
        printf '\033[1;90m[dry-run]\033[0m %s\n' "$command"
        printf 'dry-run: %s\n' "$command" > "$logfile"
        log "$label: log=$logfile"
        return 0
    fi

    log "$label: command=$command"
    SECONDS=0
    if eval "$command" >"$logfile" 2>&1; then
        elapsed=$SECONDS
        log "$label: OK (${elapsed}s, log: $logfile)"
        return 0
    else
        status=$?
    fi

    elapsed=$SECONDS
    warn "$label: failed with exit $status after ${elapsed}s (log: $logfile)"
    tail -n 20 "$logfile" >&2 || true
    return $status
}

load_release_packages() {
    local package
    PACKAGES=()
    while IFS= read -r package; do
        [[ -n "$package" ]] || continue
        PACKAGES+=("$package")
    done < <(python "$REPO_ROOT/scripts/release_manifest.py" list --release-only --field name)
}

package_tag() {
    printf '%s-v%s' "$1" "$(pkg_version "$1")"
}

pkg_version() {
    grep -m1 '^version' "packages/$1/pyproject.toml" | sed -E 's/.*"([^"]+)".*/\1/'
}

tag_exists_local() {
    git rev-parse -q --verify "refs/tags/$1" >/dev/null
}

tag_exists_remote() {
    git ls-remote --tags origin "refs/tags/$1" | grep -q "$1"
}

latest_monorepo_tag() {
    git tag -l 'v[0-9]*' | sort -V | tail -1
}

next_monorepo_tag() {
    local latest major minor patch
    latest="$(latest_monorepo_tag)"
    if [[ -z "$latest" ]]; then
        echo "v0.1.0"
        return 0
    fi
    latest="${latest#v}"
    IFS=. read -r major minor patch <<<"$latest"
    printf 'v%s.%s.%s\n' "$major" "$minor" "$((patch + 1))"
}

dirty_paths() {
    git status --porcelain | sed -E 's/^...//' | sed '/^$/d'
}

maybe_stash_dashboard() {
    local dirty before_count after_count
    [[ $AUTO_STASH_DASHBOARD -eq 1 ]] || return 0
    [[ $WORKTREE_PREPARED -eq 0 ]] || return 0

    dirty="$(dirty_paths || true)"
    if [[ -z "$dirty" ]]; then
        return 0
    fi
    if [[ "$dirty" != "DASHBOARD.txt" ]]; then
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        warn "dry-run: would stash DASHBOARD.txt to satisfy the clean-tree release gate"
        WORKTREE_PREPARED=1
        return 0
    fi

    before_count="$(git stash list | wc -l | tr -d ' ')"
    log "temporarily stashing generated DASHBOARD.txt for release"
    git stash push --include-untracked -m "release-auto-stash-dashboard" -- DASHBOARD.txt >/dev/null
    after_count="$(git stash list | wc -l | tr -d ' ')"
    if [[ "$after_count" != "$before_count" ]]; then
        STASHED_DASHBOARD=1
        STASH_REF="$(git stash list -1 --format='%gd')"
    fi
    WORKTREE_PREPARED=1
}

restore_dashboard_stash() {
    [[ $STASHED_DASHBOARD -eq 1 ]] || return 0
    log "restoring stashed DASHBOARD.txt"
    if git stash apply --index "$STASH_REF" >/dev/null 2>&1; then
        git stash drop "$STASH_REF" >/dev/null 2>&1 || true
    else
        warn "failed to restore stashed DASHBOARD.txt automatically; your stash is still available as $STASH_REF"
    fi
    STASHED_DASHBOARD=0
    STASH_REF=""
}

prepare_release_workspace() {
    maybe_stash_dashboard
    WORKTREE_PREPARED=1
}

# Git-only release state: the pushed <pkg>-vX.Y.Z tag IS the release
# (scripts/release_public.py tag-sync creates it). No PyPI query — the retired
# published-on-PyPI states were pruned with the publish path.
# Memoized set of packages whose shipped payload drifted since the version was
# set (check_release_version_drift). Computed once per process: release_state is
# called in a loop, and the drift gate walks git history for every package.
# Degrades to empty on any error (missing script, shallow clone) so read-only
# status/plan never hard-fail on the drift helper — preflight remains the gate
# that actually blocks the release.
_DRIFTED_PACKAGES_CACHE=""
_DRIFTED_PACKAGES_LOADED=0

drifted_packages() {
    if [[ $_DRIFTED_PACKAGES_LOADED -eq 0 ]]; then
        _DRIFTED_PACKAGES_LOADED=1
        _DRIFTED_PACKAGES_CACHE="$(
            python "$REPO_ROOT/scripts/check_release_version_drift.py" \
                --repo-root "$REPO_ROOT" --json 2>/dev/null \
            | python -c 'import json,sys
try:
    print(" ".join(json.load(sys.stdin).get("drifted", [])))
except Exception:
    pass' 2>/dev/null || true
        )"
    fi
    printf '%s' "$_DRIFTED_PACKAGES_CACHE"
}

is_drifted() {
    local pkg="$1" name
    for name in $(drifted_packages); do
        [[ "$name" == "$pkg" ]] && return 0
    done
    return 1
}

release_state() {
    local pkg="$1" tag local_tag remote_tag
    tag="$(package_tag "$pkg")"
    local_tag=0
    remote_tag=0
    tag_exists_local "$tag" && local_tag=1
    tag_exists_remote "$tag" && remote_tag=1

    # Payload drift means HEAD changed after the version was set. That must
    # outrank an existing remote tag, because tag-sync force-pushes the current
    # exported payload for every package family.
    if is_drifted "$pkg"; then
        echo "drifted"
    elif [[ $remote_tag -eq 1 ]]; then
        echo "released"
    elif [[ $local_tag -eq 1 ]]; then
        echo "local_tag_only"
    else
        echo "untagged"
    fi
}

print_release_status() {
    local pkg version tag state latest suggested
    latest="$(latest_monorepo_tag)"
    suggested="$(next_monorepo_tag)"
    # Prime the drift cache in THIS shell so the per-package release_state
    # subshells below inherit it (a subshell inherits parent vars at fork but
    # cannot write back), keeping the git-history drift walk to a single run.
    drifted_packages >/dev/null 2>&1 || true
    printf '%-24s %-8s %-34s %-28s\n' "PACKAGE" "VERSION" "TAG" "STATE"
    printf '%-24s %-8s %-34s %-28s\n' "------------------------" "--------" "----------------------------------" "----------------------------"
    for pkg in ${PACKAGES[@]+"${PACKAGES[@]}"}; do
        version="$(pkg_version "$pkg")"
        tag="$(package_tag "$pkg")"
        state="$(release_state "$pkg")"
        printf '%-24s %-8s %-34s %-28s\n' "$pkg" "$version" "$tag" "$state"
    done
    printf '\nlatest monorepo tag: %s\n' "${latest:-<none>}"
    printf 'suggested next tag: %s\n' "$suggested"
}

release_intended_action() {
    case "$2" in
        released) echo "skip" ;;
        drifted) echo "bump" ;;
        *) echo "release" ;;
    esac
}

release_next_safe_command() {
    local state="$2"
    case "$state" in
        released)
            echo "none (already released)"
            ;;
        drifted)
            # Payload changed without a version bump — releasing as-is would ship
            # a stale-version artifact. Bump first (one-shot coordinated bump).
            echo "make release-bump-drifted"
            ;;
        *)
            echo "make release-public FLAGS=--execute"
            ;;
    esac
}

ensure_run_log_dir() {
    if [[ -n "$RUN_LOG_DIR" ]]; then
        return 0
    fi

    RUN_ID="${WORKBAY_RELEASE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
    RUN_LOG_DIR="$REPO_ROOT/logs/release/$RUN_ID"
    mkdir -p "$RUN_LOG_DIR"
}

print_release_plan_json() {
    local requested_tag="${1:-}"
    local latest suggested plan_tag rows_file pkg version tag state action next_command exit_code

    latest="$(latest_monorepo_tag)"
    suggested="$(next_monorepo_tag)"
    plan_tag="${requested_tag:-$suggested}"
    rows_file="$(mktemp)"

    # Prime the drift cache in THIS shell (single git-history walk) so both the
    # top-level recommendation and the per-package release_state subshells below
    # read the same memoized set instead of each re-running the drift check.
    drifted_packages >/dev/null 2>&1 || true
    local top_next_command="make release-public FLAGS=--execute"
    # If any package drifted, the safe next step is a coordinated bump, not a
    # release — the primary surface must not recommend shipping stale artifacts
    # (the v0.1.37 trap: status said "untagged / release" while four packages
    # had drifted payloads).
    if [[ -n "$_DRIFTED_PACKAGES_CACHE" ]]; then
        top_next_command="make release-bump-drifted"
    fi

    for pkg in ${PACKAGES[@]+"${PACKAGES[@]}"}; do
        version="$(pkg_version "$pkg")"
        tag="$(package_tag "$pkg")"
        state="$(release_state "$pkg")"
        action="$(release_intended_action "$pkg" "$state")"
        next_command="$(release_next_safe_command "$pkg" "$state" "$plan_tag")"
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$pkg" "$version" "$tag" "$state" "$action" "$next_command" >> "$rows_file"
    done

    RELEASE_PLAN_REQUESTED_TAG="$requested_tag" \
    RELEASE_PLAN_LATEST_TAG="$latest" \
    RELEASE_PLAN_SUGGESTED_TAG="$suggested" \
    RELEASE_PLAN_NEXT_SAFE_COMMAND="$top_next_command" \
        python - "$rows_file" <<'PY'
import json
import os
import sys

packages = []
with open(sys.argv[1], encoding="utf-8") as handle:
    rows = handle.read().splitlines()
for line in rows:
    if not line:
        continue
    name, version, tag, state, intended_action, next_safe_command = line.split("\t")
    packages.append(
        {
            "name": name,
            "version": version,
            "tag": tag,
            "state": state,
            "intended_action": intended_action,
            "next_safe_command": next_safe_command,
        }
    )

plan = {
    "packages": packages,
    "monorepo": {
        "latest_tag": os.environ.get("RELEASE_PLAN_LATEST_TAG") or None,
        "requested_tag": os.environ.get("RELEASE_PLAN_REQUESTED_TAG") or None,
        "suggested_next_tag": os.environ.get("RELEASE_PLAN_SUGGESTED_TAG"),
    },
    "next_safe_command": os.environ.get("RELEASE_PLAN_NEXT_SAFE_COMMAND"),
}
print(json.dumps(plan, indent=2))
PY
    exit_code=$?
    rm -f "$rows_file"
    return "$exit_code"
}

write_release_plan_artifact() {
    local requested_tag="${1:-}"
    local plan_path plan_json

    ensure_run_log_dir
    plan_path="$RUN_LOG_DIR/plan.json"
    plan_json="$(print_release_plan_json "$requested_tag")"
    printf '%s\n' "$plan_json" > "$plan_path"
    log "plan: $plan_path"
}

write_release_summary_artifacts() {
    local command_name="$1" requested_tag="${2:-}" status="${3:-completed}" failure_log="${4:-}"
    local plan_path summary_path markdown_path

    ensure_run_log_dir
    plan_path="$RUN_LOG_DIR/plan.json"
    if [[ ! -f "$plan_path" ]]; then
        write_release_plan_artifact "$requested_tag"
    fi

    summary_path="$RUN_LOG_DIR/summary.json"
    markdown_path="$RUN_LOG_DIR/final-summary.md"

    RELEASE_SUMMARY_COMMAND="$command_name" \
    RELEASE_SUMMARY_DRY_RUN="$DRY_RUN" \
    RELEASE_SUMMARY_REQUESTED_TAG="$requested_tag" \
    RELEASE_SUMMARY_RUN_DIR="$RUN_LOG_DIR" \
    RELEASE_SUMMARY_RUN_ID="$RUN_ID" \
    RELEASE_SUMMARY_STATUS="$status" \
    RELEASE_SUMMARY_FAILURE_LOG="$failure_log" \
    RELEASE_PREFLIGHT_RESULTS_FILE="$PREFLIGHT_RESULTS_FILE" \
        python - "$plan_path" "$summary_path" "$markdown_path" <<'PY'
import json
import os
import sys

plan_path, summary_path, markdown_path = sys.argv[1:4]
with open(plan_path, encoding="utf-8") as handle:
    plan = json.load(handle)

counts = {"release": 0, "skip": 0}
for package in plan["packages"]:
    action = package["intended_action"]
    counts.setdefault(action, 0)
    counts[action] += 1

summary = {
    "run_id": os.environ["RELEASE_SUMMARY_RUN_ID"],
    "command": os.environ["RELEASE_SUMMARY_COMMAND"],
    "status": os.environ["RELEASE_SUMMARY_STATUS"],
    "dry_run": os.environ["RELEASE_SUMMARY_DRY_RUN"] == "1",
    "requested_tag": os.environ.get("RELEASE_SUMMARY_REQUESTED_TAG") or None,
    "failure_log": os.environ.get("RELEASE_SUMMARY_FAILURE_LOG") or None,
    "next_safe_command": plan.get("next_safe_command"),
    "package_counts": counts,
    "artifacts": {
        "plan": plan_path,
        "summary": summary_path,
        "final_summary": markdown_path,
    },
}

results_file = os.environ.get("RELEASE_PREFLIGHT_RESULTS_FILE")
if results_file and os.path.exists(results_file):
    with open(results_file, encoding="utf-8") as handle:
        summary["results"] = [
            json.loads(line) for line in handle.read().splitlines() if line.strip()
        ]

with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2)
    handle.write("\n")

lines = [
    "# Release Summary",
    "",
    f"- Run ID: {summary['run_id']}",
    f"- Command: {summary['command']}",
    f"- Status: {summary['status']}",
    f"- Dry run: {'yes' if summary['dry_run'] else 'no'}",
    f"- Requested tag: {summary['requested_tag'] or '<auto>'}",
    f"- Failure log: {summary['failure_log'] or '<none>'}",
    f"- Next safe command: {summary['next_safe_command']}",
    f"- Plan: {plan_path}",
    f"- Summary JSON: {summary_path}",
    "",
    "## Package Counts",
    "",
]
for key in ("release", "skip"):
    lines.append(f"- {key}: {counts.get(key, 0)}")

with open(markdown_path, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines) + "\n")
PY

    log "summary: $summary_path"
}

package_list_contains() {
    local needle="$1"
    shift || true
    local pkg
    for pkg in "$@"; do
        if [[ "$pkg" == "$needle" ]]; then
            return 0
        fi
    done
    return 1
}

preflight_failure_class() {
    case "$1" in
        0) printf 'passed' ;;
        1) printf 'genuine_test_failure' ;;
        2|3|4|5) printf 'plumbing' ;;
        *) printf 'plumbing' ;;
    esac
}

append_preflight_result() {
    local results_file="$1" pkg="$2" exit_code="$3" failure_class="$4"
    python - "$results_file" "$pkg" "$exit_code" "$failure_class" <<'PY'
import json
import sys

path, pkg, exit_code, failure_class = sys.argv[1:5]
with open(path, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {
                "pkg": pkg,
                "exit_code": int(exit_code),
                "failure_class": failure_class,
            },
            sort_keys=True,
        )
        + "\n"
    )
PY
}

ensure_clean_main() {
    prepare_release_workspace
    [[ -z "$(git status --porcelain)" ]] || die "working tree is not clean — commit or stash first."
    [[ "$(git rev-parse --abbrev-ref HEAD)" == "main" ]] || die "not on main."
    git fetch --quiet origin
    [[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/main)" ]] \
        || die "local main is not in sync with origin/main."
}

ensure_no_existing_tag() {
    local tag="$1"
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        die "tag $tag already exists locally — delete with 'git tag -d $tag' if intentional."
    fi
    if git ls-remote --tags origin "refs/tags/$tag" | grep -q "$tag"; then
        die "tag $tag already exists on origin — releases are bump-and-fix, not re-publish."
    fi
}

cleanup() {
    restore_dashboard_stash
}

trap cleanup EXIT

load_release_packages

# ----- subcommands -----------------------------------------------------------

cmd_preflight() {
    local test_packages=("$@")
    local started_run=0
    LAST_FAILURE_LOG=""
    if [[ ${#test_packages[@]} -eq 0 ]]; then
        test_packages=(${PACKAGES[@]+"${PACKAGES[@]}"})
    fi

    if [[ -z "$RUN_LOG_DIR" ]]; then
        ensure_run_log_dir
        started_run=1
    fi

    log "preflight: working-tree state"
    ensure_clean_main

    if [[ $SKIP_TESTS -eq 0 ]]; then
        log "preflight: per-package test-preflight ports"
        PREFLIGHT_RESULTS_FILE="$RUN_LOG_DIR/preflight-results.jsonl"
        : > "$PREFLIGHT_RESULTS_FILE"
        for pkg in "${test_packages[@]}"; do
            local logfile="$RUN_LOG_DIR/preflight-$pkg.log"
            local exit_file="$RUN_LOG_DIR/preflight-$pkg.exit"
            local status=0
            local exit_code=0
            local failure_class
            rm -f "$exit_file"
            if run_logged \
                "preflight: $pkg" \
                "$logfile" \
                "PYTHONPATH='$REPO_ROOT' WORKBAY_PREFLIGHT_EXIT_CODE_FILE='$exit_file' make -C packages/$pkg test-preflight"; then
                status=0
            else
                status=$?
            fi
            if [[ -f "$exit_file" ]]; then
                exit_code="$(tr -d '[:space:]' < "$exit_file")"
            else
                exit_code="$status"
            fi
            failure_class="$(preflight_failure_class "$exit_code")"
            append_preflight_result "$PREFLIGHT_RESULTS_FILE" "$pkg" "$exit_code" "$failure_class"
            if [[ $status -ne 0 ]]; then
                LAST_FAILURE_LOG="$logfile"
                if [[ $started_run -eq 1 ]]; then
                    write_release_summary_artifacts "preflight" "" "failed" "$LAST_FAILURE_LOG"
                fi
                return 1
            fi
        done
    else
        warn "preflight: --skip-tests set, skipping per-package suites"
    fi

    log "preflight: pyproject.toml VCS-dep scan"
    if grep -RnE "^\s*['\"]?[a-zA-Z0-9_-]+\s*@\s*git\+" packages/*/pyproject.toml; then
        warn "found direct VCS dependency in a pyproject.toml — replace with a version range before release."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: release version-drift gate"
    if ! python scripts/check_release_version_drift.py; then
        warn "a publishable package's shipped payload changed since its version was set — bump it before releasing."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: workbay anchor pin gate"
    if ! python scripts/stack_pins.py check; then
        warn "workbay pins drift from member versions — run 'make stack-pins-sync' (+ version bump/CHANGELOG) before releasing."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: CHANGELOG-ready gate"
    if ! python scripts/check_changelog_ready.py; then
        warn "a releasing package's CHANGELOG entry is missing or a bare TODO stub — the public export would ship a blank entry. Fill it in before releasing."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: public-export forbidden-token gate"
    if ! python scripts/export_public.py --check; then
        warn "the public export would leak a forbidden token/path (e.g. the private repo name) — scrub it before releasing (this is the gate that aborted v0.1.37 at --execute)."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: stale-higher tag audit (read-only)"
    local logfile="$RUN_LOG_DIR/preflight-audit-tags.log"
    if ! run_logged \
        "preflight: audit-tags" \
        "$logfile" \
        "python scripts/release_public.py audit-tags"; then
        warn "pre-rebrand stale-higher tags detected — run 'python scripts/prune_stale_tags.py' (dry-run) then --execute."
        LAST_FAILURE_LOG="$logfile"
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed" "$LAST_FAILURE_LOG"
        fi
        return 1
    fi

    log "preflight: shipped-surface privacy gate (built artifacts)"
    if ! python scripts/check_shipped_privacy.py; then
        warn "a built wheel/sdist ships personal info or internal project ids — fix the scrub-at-build path (+ version bump/CHANGELOG) before releasing."
        if [[ $started_run -eq 1 ]]; then
            write_release_summary_artifacts "preflight" "" "failed"
        fi
        return 1
    fi

    log "preflight: OK"

    if [[ $started_run -eq 1 ]]; then
        write_release_summary_artifacts "preflight"
    fi
}

cmd_status() {
    print_release_status
}

cmd_plan() {
    local requested_tag="${1:-}"

    if [[ $JSON_OUTPUT -eq 1 ]]; then
        print_release_plan_json "$requested_tag"
        return 0
    fi

    print_release_status
}

cmd_monorepo() {
    local tag="${1:-}"
    local started_run=0
    [[ -n "$tag" ]] || die "usage: release.sh monorepo <vX.Y.Z>"
    [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "monorepo tag must look like vX.Y.Z (got: $tag)"

    if [[ -z "$RUN_LOG_DIR" ]]; then
        write_release_plan_artifact "$tag"
        started_run=1
    fi

    ensure_clean_main
    ensure_no_existing_tag "$tag"

    log "monorepo: confirming all package tags are reachable from HEAD"
    for pkg in ${PACKAGES[@]+"${PACKAGES[@]}"}; do
        local version pkg_tag
        version="$(pkg_version "$pkg")"
        pkg_tag="${pkg}-v${version}"
        if ! git rev-parse -q --verify "refs/tags/$pkg_tag" >/dev/null; then
            die "missing per-package tag $pkg_tag — run 'make release-public FLAGS=--execute' to push the per-package tags first."
        fi
        if ! git merge-base --is-ancestor "$pkg_tag" HEAD; then
            die "$pkg_tag is not an ancestor of HEAD — package tags must share the commit chain with the monorepo tag."
        fi
    done

    log "monorepo: cutting $tag"
    run "git tag $tag"
    run "git push origin $tag"

    log "monorepo: smoke-testing one-command install against /tmp/release-smoke-$$-$(date +%s)"
    local smoke_dir="/tmp/release-smoke-$$-$(date +%s)"
    run "mkdir -p $smoke_dir && cd $smoke_dir && git init -q"
    run "uvx --from 'git+https://github.com/darce/workbay@${tag}#subdirectory=packages/workbay-bootstrap' workbay-bootstrap install --target $smoke_dir"
    log "monorepo: smoke install succeeded — inspect $smoke_dir manually to confirm overlay surfaces."

    if [[ $started_run -eq 1 ]]; then
        write_release_summary_artifacts "monorepo" "$tag"
    fi
}

# ----- dispatch --------------------------------------------------------------

case "$SUBCOMMAND" in
    preflight) shift || true; cmd_preflight ;;
    plan)      shift || true; cmd_plan "${1:-}" ;;
    status)    cmd_status ;;
    monorepo)  shift; cmd_monorepo "${1:-}" ;;
    "")        die "no subcommand. Try: $0 --help" ;;
    *)         die "unknown subcommand: $SUBCOMMAND. Try: $0 --help" ;;
esac
