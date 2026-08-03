#!/usr/bin/env bash
# Deterministic flock-dispatch primitive for grok-remote lanes.
#
# Hand-rolled dispatch.sh scripts repeatedly made the same three mistakes that
# silently burn budget or mis-report success:
#
#   1. Omitting --out for an EDIT lane. remote_agent.sh then writes the patch on
#      stdout; if the caller redirects stdout to a per-lane log, the committed
#      work is buried and the run looks like "success, nothing produced".
#   2. Treating exit 4 ("no committed changes") the same for both lane kinds.
#      For REVIEW it is the expected success code; for EDIT it is total failure.
#   3. Dispatching two lanes against the same branch. LANE_KEY is derived from
#      the branch alone, so same-branch lanes serialize on the lane lock —
#      safe, but not the concurrency the operator paid for.
#   4. Omitting --schema. remote_agent.sh hard-requires it and dies before the
#      VM with "--schema file not found: <unset>". Defaults are kind-specific
#      (edit = lane_result schema, review = review_runner schema) and live in
#      --out-dir; an optional fifth TSV column overrides per lane.
#
# This primitive validates the whole flock, dispatches each lane through
# remote_agent.sh (or $REMOTE_AGENT), interprets exit codes by kind, and prints
# a per-lane summary so a half-failed flock cannot be mistaken for success.
#
# Usage:
#   scripts/offload_flock.sh --manifest <tsv> --out-dir <dir>
#
# Manifest (TSV, no header, four or five columns):
#   lane_id <TAB> kind <TAB> brief_path <TAB> branch [<TAB> schema_path]
#
# kind is review|edit. Validation refuses unknown kinds, missing brief paths,
# unresolvable schema overrides, and duplicate branches before any lane is
# dispatched. Four-column rows use the kind default schema; a fifth column
# names a per-lane schema file and wins over the default.
#
# Exit: 0 if every lane succeeded · nonzero if validation failed or any lane
# failed (including edit lanes with empty / missing patches).
set -euo pipefail

die() {
    echo "offload_flock: $*" >&2
    exit 2
}

usage() {
    cat >&2 <<'EOF'
Usage: scripts/offload_flock.sh --manifest <tsv> --out-dir <dir>

Manifest TSV columns (no header):
  lane_id  kind  brief_path  branch  [schema_path]

kind is review or edit.
schema_path is optional; when omitted the kind default is used
(edit: lane_result schema; review: review_runner schema), materialized
into --out-dir. A fifth column that names a missing file is refused
before any lane is dispatched.
EOF
    exit 2
}

manifest=""
out_dir=""

while [ $# -gt 0 ]; do
    case "$1" in
        --manifest)
            manifest="${2:-}"
            shift 2
            ;;
        --out-dir)
            out_dir="${2:-}"
            shift 2
            ;;
        -h | --help)
            usage
            ;;
        *)
            die "unknown argument: $1"
            ;;
    esac
done

[ -n "$manifest" ] || die "--manifest required"
[ -n "$out_dir" ] || die "--out-dir required"
[ -f "$manifest" ] || die "manifest not found: $manifest"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
remote_agent="${REMOTE_AGENT:-${script_dir}/remote_agent.sh}"

declare -a lane_ids=()
declare -a kinds=()
declare -a briefs=()
declare -a branches=()
declare -a schema_overrides=()

# Read and validate the entire manifest before dispatching anything.
line_no=0
while IFS=$'\t' read -r lane_id kind brief_path branch schema_path || [ -n "${lane_id:-}" ]; do
    line_no=$((line_no + 1))
    # Skip blank lines (trailing newline produces one empty read).
    if [ -z "${lane_id:-}" ] && [ -z "${kind:-}" ] && [ -z "${brief_path:-}" ] && [ -z "${branch:-}" ] && [ -z "${schema_path:-}" ]; then
        continue
    fi
    if [ -z "${lane_id:-}" ] || [ -z "${kind:-}" ] || [ -z "${brief_path:-}" ] || [ -z "${branch:-}" ]; then
        die "manifest line ${line_no}: expected four or five tab-separated columns (lane_id kind brief_path branch [schema_path])"
    fi
    case "$kind" in
        review | edit) ;;
        *)
            die "unknown kind: ${kind} (lane ${lane_id}; only 'review' and 'edit' are allowed)"
            ;;
    esac
    if [ ! -f "$brief_path" ]; then
        die "brief path does not exist: ${brief_path} (lane ${lane_id})"
    fi
    if [ -n "${schema_path:-}" ] && [ ! -f "$schema_path" ]; then
        die "schema path does not exist: ${schema_path} (lane ${lane_id})"
    fi
    lane_ids+=("$lane_id")
    kinds+=("$kind")
    briefs+=("$brief_path")
    branches+=("$branch")
    schema_overrides+=("${schema_path:-}")
done <"$manifest"

[ "${#lane_ids[@]}" -gt 0 ] || die "manifest is empty: $manifest"

# Refuse duplicate branches before any dispatch: same-branch lanes share a
# LANE_KEY and serialize, which defeats the purpose of a flock.
# Linear scan (bash 3.2 — no associative arrays); flock lane counts are small.
for i in "${!lane_ids[@]}"; do
    b="${branches[$i]}"
    for j in "${!lane_ids[@]}"; do
        if [ "$j" -ge "$i" ]; then
            break
        fi
        if [ "${branches[$j]}" = "$b" ]; then
            die "duplicate branch: ${b} (lanes ${lane_ids[$j]} and ${lane_ids[$i]})"
        fi
    done
done

mkdir -p "$out_dir"

# Materialize kind-default schemas into --out-dir (inspectable, cleaned with
# flock output). Owners are the Python modules; do not re-spell the JSON here.
# Interpreter selection matches scripts/handoff_write.sh.
python="${repo_root}/.venv/bin/python"
if [ ! -x "$python" ]; then
    die "interpreter not found: ${python} (required to materialize lane result schemas)"
fi
default_edit_schema="${out_dir}/edit-result.schema.json"
default_review_schema="${out_dir}/review-output.schema.json"
lane_result_py="${repo_root}/packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/lane_result.py"
review_runner_py="${repo_root}/packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/review_runner.py"
if ! "$python" "$lane_result_py" schema >"$default_edit_schema"; then
    die "failed to materialize edit schema via ${python}"
fi
if ! "$python" "$review_runner_py" schema >"$default_review_schema"; then
    die "failed to materialize review schema via ${python}"
fi

# Outcomes for the operator summary (lane_id + result text).
declare -a summary_lines=()
any_failed=0

dispatch_lane() {
    local lane_id="$1"
    local kind="$2"
    local brief_path="$3"
    local branch="$4"
    local schema="$5"
    local result_out="${out_dir}/${lane_id}.result.json"
    local patch_out="${out_dir}/${lane_id}.patch"
    local -a cmd
    local rc=0

    cmd=("$remote_agent" build --branch "$branch" --brief "$brief_path" --schema "$schema" --result-out "$result_out")
    if [ "$kind" = "edit" ]; then
        cmd+=(--out "$patch_out")
    fi

    # Capture exit code without tripping set -e.
    rc=0
    "${cmd[@]}" || rc=$?

    if [ "$kind" = "review" ]; then
        # Exit 4 ("no committed changes") is the expected success for review.
        if [ "$rc" -eq 0 ] || [ "$rc" -eq 4 ]; then
            summary_lines+=("${lane_id}: ok (review, exit ${rc})")
            return 0
        fi
        echo "offload_flock: lane ${lane_id}: failed (review, exit ${rc})" >&2
        summary_lines+=("${lane_id}: FAIL (review, exit ${rc})")
        return 1
    fi

    # edit lane
    if [ "$rc" -eq 4 ]; then
        echo "offload_flock: lane ${lane_id}: failed — no committed changes (exit 4)" >&2
        summary_lines+=("${lane_id}: FAIL (edit, no committed changes)")
        return 1
    fi
    if [ "$rc" -ne 0 ]; then
        echo "offload_flock: lane ${lane_id}: failed (edit, exit ${rc})" >&2
        summary_lines+=("${lane_id}: FAIL (edit, exit ${rc})")
        return 1
    fi
    # Exit 0 but missing or zero-byte patch is still a failure for edit lanes.
    if [ ! -s "$patch_out" ]; then
        echo "offload_flock: lane ${lane_id}: empty patch / no patch" >&2
        summary_lines+=("${lane_id}: FAIL (empty patch / no patch)")
        return 1
    fi
    summary_lines+=("${lane_id}: ok (edit)")
    return 0
}

for i in "${!lane_ids[@]}"; do
    schema="${schema_overrides[$i]}"
    if [ -z "$schema" ]; then
        if [ "${kinds[$i]}" = "edit" ]; then
            schema="$default_edit_schema"
        else
            schema="$default_review_schema"
        fi
    fi
    if ! dispatch_lane "${lane_ids[$i]}" "${kinds[$i]}" "${briefs[$i]}" "${branches[$i]}" "$schema"; then
        any_failed=1
    fi
done

echo "offload_flock: summary"
for line in "${summary_lines[@]}"; do
    echo "  ${line}"
done

if [ "$any_failed" -ne 0 ]; then
    echo "offload_flock: one or more lanes failed" >&2
    exit 1
fi
exit 0
