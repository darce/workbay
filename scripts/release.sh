#!/usr/bin/env bash
#
# Release driver for the WorkBay repository (git-only delivery).
#
# Usage:
#   scripts/release.sh package <name>            # build + tag + push the package tag (git-only)
#   scripts/release.sh monorepo <vX.Y.Z>         # cut the consumer-facing monorepo tag
#   scripts/release.sh pending [vX.Y.Z]          # tag unreleased package versions, then cut the monorepo tag
#   scripts/release.sh plan [vX.Y.Z] [--json]    # print the computed release plan
#   scripts/release.sh status                    # show release/tag status for each package
#   scripts/release.sh all                       # tag all packages in dep order, then prompt for monorepo tag
#   scripts/release.sh preflight                 # run the pre-release checklist only
#
# Flags:
#   --dry-run       print what would be done, take no destructive action
#   --allow-pre     allow tagging versions with PEP 440 pre-release suffixes (.devN, aN, bN, rcN)
#   --skip-tests    skip the per-package pytest run (still runs contract + rehearsal)
#   --auto-stash-dashboard   temporarily stash a dirty DASHBOARD.txt during release checks/tagging
#
# Delivery: git-only. PyPI publishing is retired; the public mirror sync and
# tag-sync (scripts/release_public.py) are the sole release action.
#
# Authoritative playbook: docs/RELEASING.md.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# ----- argument parsing ------------------------------------------------------

DRY_RUN=0
ALLOW_PRE=0
SKIP_TESTS=0
AUTO_STASH_DASHBOARD=0
JSON_OUTPUT=0
POSITIONAL=()

PACKAGES=()
PENDING_RELEASE_PACKAGES=()
PLANNED_RELEASE_PACKAGES=()
WORKTREE_PREPARED=0
STASHED_DASHBOARD=0
STASH_REF=""
RUN_ID=""
RUN_LOG_DIR=""
LAST_FAILURE_LOG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --allow-pre) ALLOW_PRE=1; shift ;;
        --skip-tests) SKIP_TESTS=1; shift ;;
        --auto-stash-dashboard) AUTO_STASH_DASHBOARD=1; shift ;;
        --json) JSON_OUTPUT=1; shift ;;
        -h|--help)
            sed -n '3,16p' "${BASH_SOURCE[0]}"; exit 0 ;;
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

manifest_value() {
    python "$REPO_ROOT/scripts/release_manifest.py" get "$1" "$2"
}

load_release_packages() {
    local package
    PACKAGES=()
    while IFS= read -r package; do
        [[ -n "$package" ]] || continue
        PACKAGES+=("$package")
    done < <(python "$REPO_ROOT/scripts/release_manifest.py" list --release-only --field name)
}

artifact_prefix() {
    manifest_value "$1" artifact_prefix
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

published_on_pypi() {
    local pkg="$1" version="$2"
    uvx --quiet pip index versions "$pkg" 2>/dev/null \
        | tr ',()' '   ' \
        | tr ' ' '\n' \
        | grep -Fxq "$version"
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

release_state() {
    local pkg="$1" version tag local_tag remote_tag published
    version="$(pkg_version "$pkg")"
    tag="$(package_tag "$pkg")"
    local_tag=0
    remote_tag=0
    published=0
    tag_exists_local "$tag" && local_tag=1
    tag_exists_remote "$tag" && remote_tag=1
    published_on_pypi "$pkg" "$version" && published=1

    if [[ $published -eq 1 ]]; then
        if [[ $remote_tag -eq 1 ]]; then
            echo "released"
        elif [[ $local_tag -eq 1 ]]; then
            echo "local_tag_with_pypi_missing_remote"
        else
            echo "pypi_without_tag"
        fi
        return 0
    fi

    if [[ $remote_tag -eq 1 ]]; then
        echo "remote_tag_without_pypi"
    elif [[ $local_tag -eq 1 ]]; then
        echo "local_tag_only"
    else
        echo "pending_upload"
    fi
}

print_release_status() {
    local pkg version tag state latest suggested
    latest="$(latest_monorepo_tag)"
    suggested="$(next_monorepo_tag)"
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

pending_packages() {
    PENDING_RELEASE_PACKAGES=()
    local pkg state
    for pkg in ${PACKAGES[@]+"${PACKAGES[@]}"}; do
        state="$(release_state "$pkg")"
        if [[ "$state" == "pending_upload" ]]; then
            PENDING_RELEASE_PACKAGES+=("$pkg")
        elif [[ "$state" != "released" ]]; then
            die "cannot continue: $pkg is in anomalous release state '$state' ($(release_recovery_guidance "$pkg" "$state"))."
        fi
    done
}

release_recovery_guidance() {
    local pkg="$1" state="$2"
    case "$state" in
        pypi_without_tag)
            echo "create and push the matching package tag before continuing"
            ;;
        local_tag_with_pypi_missing_remote)
            echo "push the matching package tag to origin before continuing"
            ;;
        *)
            echo "fix tags/PyPI first"
            ;;
    esac
}

release_intended_action() {
    case "$2" in
        released) echo "skip" ;;
        pending_upload) echo "publish" ;;
        *) echo "block" ;;
    esac
}

release_next_safe_command() {
    local pkg="$1" state="$2" requested_tag="$3"
    case "$state" in
        released|pending_upload)
            echo "scripts/release.sh pending $requested_tag"
            ;;
        *)
            echo "$(release_recovery_guidance "$pkg" "$state")"
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
    RELEASE_PLAN_NEXT_SAFE_COMMAND="scripts/release.sh pending $plan_tag" \
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
        python - "$plan_path" "$summary_path" "$markdown_path" <<'PY'
import json
import os
import sys

plan_path, summary_path, markdown_path = sys.argv[1:4]
with open(plan_path, encoding="utf-8") as handle:
    plan = json.load(handle)

counts = {"publish": 0, "skip": 0, "block": 0}
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
for key in ("publish", "skip", "block"):
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

is_pre_release() {
    [[ "$1" =~ (\.dev[0-9]+|a[0-9]+|b[0-9]+|rc[0-9]+)$ ]]
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

ensure_no_published_version() {
    local pkg="$1" version="$2"
    if published_on_pypi "$pkg" "$version"; then
        die "$pkg $version is already on PyPI. Bump the version and try again."
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
        log "preflight: per-package test suites"
        for pkg in "${test_packages[@]}"; do
            local logfile="$RUN_LOG_DIR/preflight-$pkg.log"
            if ! run_logged \
                "preflight: $pkg" \
                "$logfile" \
                "(cd packages/$pkg && python -m pytest -q)"; then
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

    if package_list_contains workbay-protocol "${test_packages[@]}" \
        || package_list_contains mcp-workbay-handoff "${test_packages[@]}" \
        || package_list_contains mcp-workbay-orchestrator "${test_packages[@]}"; then
        log "preflight: cross-package contract test"
        local logfile="$RUN_LOG_DIR/preflight-contract.log"
        if ! run_logged \
            "preflight: contract" \
            "$logfile" \
            "(cd packages/mcp-workbay-orchestrator && python -m pytest tests/test_protocol_contract.py -q)"; then
            LAST_FAILURE_LOG="$logfile"
            if [[ $started_run -eq 1 ]]; then
                write_release_summary_artifacts "preflight" "" "failed" "$LAST_FAILURE_LOG"
            fi
            return 1
        fi
    fi

    if package_list_contains workbay-bootstrap "${test_packages[@]}"; then
        log "preflight: bootstrap install rehearsal"
        local logfile="$RUN_LOG_DIR/preflight-bootstrap-rehearsal.log"
        if ! run_logged \
            "preflight: bootstrap rehearsal" \
            "$logfile" \
            "(cd packages/workbay-bootstrap && python -m pytest tests/test_bootstrap_install_rehearsal.py -q)"; then
            LAST_FAILURE_LOG="$logfile"
            if [[ $started_run -eq 1 ]]; then
                write_release_summary_artifacts "preflight" "" "failed" "$LAST_FAILURE_LOG"
            fi
            return 1
        fi
    fi

    log "preflight: pyproject.toml VCS-dep scan"
    if grep -RnE "^\s*['\"]?[a-zA-Z0-9_-]+\s*@\s*git\+" packages/*/pyproject.toml; then
        warn "found direct VCS dependency in a pyproject.toml — replace with PyPI version range before release."
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

check_sde_signoff() {
    local pkg="$1"
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '\033[1;90m[dry-run]\033[0m %s\n' \
            "python scripts/published_bytes_sde_signoff.py --require-signoff $pkg"
        return 0
    fi
    if ! python scripts/published_bytes_sde_signoff.py --require-signoff "$pkg"; then
        die "SDE sign-off missing for $pkg — run: python scripts/published_bytes_sde_signoff.py --package $pkg --record"
    fi
}

build_gate_validated_dist() {
    local pkg="$1" source_date_epoch="$2"
    run "rm -rf packages/$pkg/dist/"
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '\033[1;90m[dry-run]\033[0m %s\n' \
            "(cd packages/$pkg && SOURCE_DATE_EPOCH=$source_date_epoch uvx --from build pyproject-build)"
    else
        run "(cd packages/$pkg && SOURCE_DATE_EPOCH=$source_date_epoch uvx --from build pyproject-build)"
    fi
    if [[ $DRY_RUN -eq 1 ]]; then
        printf '\033[1;90m[dry-run]\033[0m %s\n' \
            "python scripts/check_shipped_privacy.py --package $pkg --dist-dir packages/$pkg/dist"
    elif ! python scripts/check_shipped_privacy.py --package "$pkg" --dist-dir "packages/$pkg/dist"; then
        die "shipped-surface privacy gate failed — a built wheel/sdist ships personal info or internal project ids; fix scrub-at-build (+ bump/CHANGELOG) before releasing."
    fi
}

cmd_package() {
    local pkg="${1:-}"
    [[ -n "$pkg" ]] || die "usage: release.sh package <name>"
    [[ -d "packages/$pkg" ]] || die "no such package: packages/$pkg"

    local version tag started_run=0
    version="$(pkg_version "$pkg")"
    tag="${pkg}-v${version}"

    if is_pre_release "$version" && [[ $ALLOW_PRE -eq 0 ]]; then
        die "$pkg pyproject version is $version (pre-release). Bump to a stable version, or pass --allow-pre."
    fi

    if [[ -z "$RUN_LOG_DIR" ]]; then
        write_release_plan_artifact
        started_run=1
    fi

    log "release: $pkg $version (tag: $tag)"
    ensure_clean_main
    ensure_no_existing_tag "$tag"
    ensure_no_published_version "$pkg" "$version"

    # The single-package release path must also gate on version drift — otherwise
    # `release-package` ships payload changes with no bump (the 0019 bug class).
    # Scoped to "$pkg": a decoupled single-package release (e.g. the workbay
    # name-claim) must not be blocked by an unrelated package's unbumped drift.
    # The multi-package preflight (release.sh pending/all) keeps the repo-wide check.
    if ! python scripts/check_release_version_drift.py --package "$pkg"; then
        die "release version-drift gate failed — $pkg's shipped payload changed since its version was set; bump it (+ CHANGELOG) before releasing."
    fi

    # Same fail-closed posture for the version anchor: a member must never
    # publish while the workbay pins still say the previous version.
    if ! python scripts/stack_pins.py check; then
        die "workbay pin gate failed — run 'make stack-pins-sync' (+ version bump/CHANGELOG) so the anchor pins this release."
    fi

    # Pin archive timestamps so the locally gated dist is bit-comparable to the
    # runner rebuild on darce/workbay (implementation note A1-bit).
    local source_date_epoch
    source_date_epoch="$(git log -1 --format=%ct HEAD)"

    check_sde_signoff "$pkg"
    build_gate_validated_dist "$pkg" "$source_date_epoch"

    log "release: tagging $tag"
    run "git tag $tag"
    run "git push origin $tag"

    # Git-only delivery: the pushed package tag IS the release. PyPI publishing
    # is retired — the public mirror sync + tag-sync (scripts/release_public.py)
    # carry the dist to consumers; there is no upload step here.
    log "release: pushed package tag $tag (git-only delivery; no PyPI publish)"

    if [[ $started_run -eq 1 ]]; then
        write_release_summary_artifacts "package"
    fi
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
            if [[ $DRY_RUN -eq 1 ]] && package_list_contains "$pkg" "${PLANNED_RELEASE_PACKAGES[@]:-}"; then
                warn "dry-run: assuming planned package tag $pkg_tag will exist before cutting $tag"
                continue
            fi
            die "missing per-package tag $pkg_tag — release each package first."
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

cmd_pending() {
    local requested_tag="${1:-}"
    local tag pending pkg
    pending=()

    if [[ -n "$requested_tag" && ! "$requested_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        die "pending release tag must look like vX.Y.Z (got: $requested_tag)"
    fi

    log "pending: checking package/tag/PyPI state"
    print_release_status
    pending_packages
    if [[ ${#PENDING_RELEASE_PACKAGES[@]} -gt 0 ]]; then
        pending=("${PENDING_RELEASE_PACKAGES[@]}")
    fi
    tag="${requested_tag:-$(next_monorepo_tag)}"
    PLANNED_RELEASE_PACKAGES=("${pending[@]:-}")
    write_release_plan_artifact "$tag"

    if [[ ${#pending[@]} -eq 0 ]]; then
        if tag_exists_local "$tag" || tag_exists_remote "$tag"; then
            warn "all package versions are already released and monorepo tag $tag already exists."
            write_release_summary_artifacts "pending" "$tag"
            return 0
        fi

        warn "all package versions are already released; cutting missing monorepo tag $tag."
        cmd_monorepo "$tag"
        write_release_summary_artifacts "pending" "$tag"
        return 0
    fi

    if ! cmd_preflight "${pending[@]}"; then
        write_release_summary_artifacts "pending" "$tag" "failed" "$LAST_FAILURE_LOG"
        return 1
    fi

    for pkg in "${pending[@]}"; do
        cmd_package "$pkg"
    done

    cmd_monorepo "$tag"
    write_release_summary_artifacts "pending" "$tag"
}

cmd_all() {
    local pending pkg
    pending=()

    log "all: checking package/tag/PyPI state"
    print_release_status
    pending_packages
    if [[ ${#PENDING_RELEASE_PACKAGES[@]} -gt 0 ]]; then
        pending=("${PENDING_RELEASE_PACKAGES[@]}")
    fi
    PLANNED_RELEASE_PACKAGES=("${pending[@]:-}")
    write_release_plan_artifact

    if [[ ${#pending[@]} -eq 0 ]]; then
        warn "all package versions are already released. Run: scripts/release.sh monorepo vX.Y.Z to cut the consumer tag."
        write_release_summary_artifacts "all"
        return 0
    fi

    if ! cmd_preflight "${pending[@]}"; then
        write_release_summary_artifacts "all" "" "failed" "$LAST_FAILURE_LOG"
        return 1
    fi
    for pkg in "${pending[@]}"; do
        cmd_package "$pkg"
    done
    warn "all pending packages released. Run: scripts/release.sh monorepo vX.Y.Z to cut the consumer tag."
    write_release_summary_artifacts "all"
}

# ----- dispatch --------------------------------------------------------------

case "$SUBCOMMAND" in
    preflight) shift || true; cmd_preflight ;;
    plan)      shift || true; cmd_plan "${1:-}" ;;
    status)    cmd_status ;;
    pending)   shift || true; cmd_pending "${1:-}" ;;
    package)       shift; cmd_package "${1:-}" ;;
    monorepo)      shift; cmd_monorepo "${1:-}" ;;
    all)       cmd_all ;;
    "")        die "no subcommand. Try: $0 --help" ;;
    *)         die "unknown subcommand: $SUBCOMMAND. Try: $0 --help" ;;
esac
