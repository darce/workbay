#!/usr/bin/env bash
# Deterministic flock-dispatch primitive for remote lanes (implementation note S4/S6).
#
# Validates the flock, builds per-lane AgentSpecs, pre-charges a token budget,
# dispatches through remote_agent.sh --agent-spec, and writes flock-receipt.json
# on every exit path (including validation abort).
#
# Usage:
#   scripts/offload_flock.sh --manifest <tsv> --out-dir <dir> \
#       [--lane-timeout <s>] [--token-budget <n>]
set -euo pipefail

manifest=""
out_dir=""
lane_timeout_override=""
token_budget=""
flock_exit=2
aborted=""
charged_total=0
started_at=""
receipt_lanes_ndjson=""
any_failed=0
refuse_rest=0

die() {
    echo "offload_flock: $*" >&2
    aborted="${aborted:-validation}"
    flock_exit=2
    _write_receipt || true
    exit 2
}

usage() {
    cat >&2 <<'EOF'
Usage: scripts/offload_flock.sh --manifest <tsv> --out-dir <dir>
       [--lane-timeout <s>] [--token-budget <n>]

Manifest TSV columns (no header):
  lane_id  kind  brief_path  branch  [schema_path]
    backend  [model]  [effort]  [est_tokens]  [speed]

kind is review or edit.
backend, model and effort default via backend_spec when the column is empty.
speed is optional and codex-remote only (standard or fast).
When --token-budget is set, every lane needs a positive est_tokens
(column or backend DEFAULT_EST_TOKENS).
EOF
    exit 2
}

_write_receipt() {
    [ -n "${out_dir:-}" ] || return 0
    [ -n "${started_at:-}" ] || return 0
    [ -x "${python:-}" ] || return 0
    [ -n "${backend_spec_py:-}" ] || return 0
    mkdir -p "$out_dir"
    local finished payload
    finished="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    payload="${out_dir}/.receipt-payload.json"
    RECEIPT_PAYLOAD="$payload" \
    RECEIPT_LANES_FILE="${receipt_lanes_ndjson}" \
    RECEIPT_MANIFEST="$manifest" \
    RECEIPT_OUT_DIR="$out_dir" \
    RECEIPT_STARTED="$started_at" \
    RECEIPT_FINISHED="$finished" \
    RECEIPT_EXIT="$flock_exit" \
    RECEIPT_ABORTED="${aborted:-}" \
    RECEIPT_BUDGET="${token_budget:-}" \
    RECEIPT_CHARGED="$charged_total" \
    "$python" -c '
import json, os
from pathlib import Path
lanes = []
p = Path(os.environ["RECEIPT_LANES_FILE"])
if p.is_file():
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            lanes.append(json.loads(line))
aborted = os.environ.get("RECEIPT_ABORTED") or None
budget_raw = os.environ.get("RECEIPT_BUDGET") or ""
budget = int(budget_raw) if budget_raw != "" else None
doc = {
    "receipt_version": 1,
    "manifest_path": os.environ["RECEIPT_MANIFEST"],
    "out_dir": os.environ["RECEIPT_OUT_DIR"],
    "started_at": os.environ["RECEIPT_STARTED"],
    "finished_at": os.environ["RECEIPT_FINISHED"],
    "flock_exit": int(os.environ["RECEIPT_EXIT"]),
    "aborted": aborted,
    "token_budget": budget,
    "charged_total": int(os.environ["RECEIPT_CHARGED"]),
    "lanes": lanes,
}
Path(os.environ["RECEIPT_PAYLOAD"]).write_text(json.dumps(doc) + "\n")
' || return 0
    "$python" "$backend_spec_py" write-receipt --out "${out_dir}/flock-receipt.json" --payload "$payload" >/dev/null || true
}

_append_lane_receipt() {
    # Args: json object as single argument
    printf '%s\n' "$1" >>"$receipt_lanes_ndjson"
}

_append_synthetic_not_dispatched() {
    # Args: reason string (may mention a line number)
    local reason="$1"
    _append_lane_receipt "$(
        "$python" -c '
import json, sys
print(json.dumps({
  "lane_id": "synthetic", "kind": "edit", "backend_id": "",
  "model": "", "effort": "", "service_tier": None, "lane_timeout_s": 0,
  "state": "not_dispatched", "dispatched_at": None, "finished_at": None,
  "agent_exit_code": None, "reason": sys.argv[1],
  "est_tokens": 0, "observed_tokens": None,
  "token_provenance": None, "charged_tokens": None,
  "estimate_overshoot": False, "result_parse": None, "operator_action": None,
}))
' "$reason"
    )"
}

# Filename slug for lane_id. Lane ids may contain '/' (e.g. lane/rev-foo);
# interpolating them into paths creates missing parent dirs. Keep $lane_id
# itself as the identity value; only filenames use the slug. Strip surrounding
# whitespace so " a/b " and "a/b" do not fork a second slug.
_lane_slug() {
    printf %s "$1" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | tr "/" "_"
}

# Exit-3 / exit-4 salvage: keep a non-empty --out, otherwise emit
# format-patch of an explicit recorded BASE..HEAD range (never a lone HEAD:
# git treats `format-patch HEAD` as <since> and always emits 0 bytes) and, if
# HEAD is unchanged, the index + worktree diff. Dest is the per-lane patch
# beside result_out. Always returns 0 so callers keep original exit-code
# semantics. Missing git_src / non-repo / mktemp failure is a logged no-op
# — do not claim salvage, and never call `git -C ""` (that resolves to cwd).
_salvage_edit_patch() {
    local dest="$1"
    local git_src="${2:-}"
    local salvage_base="${3:-${BASE:-}}"
    [ -s "$dest" ] && return 0
    if [ -z "$git_src" ]; then
        echo "offload_flock: salvage skipped: no git_src" >&2
        return 0
    fi
    if ! git -C "$git_src" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        echo "offload_flock: salvage skipped: not a git repo" >&2
        return 0
    fi
    local tmp
    tmp="$(mktemp "${TMPDIR:-/tmp}/offload_flock_salvage.XXXXXX")" || {
        echo "offload_flock: salvage skipped: mktemp failed" >&2
        return 0
    }
    if [ -n "$salvage_base" ]; then
        [ -n "$git_src" ] && git -C "$git_src" format-patch --no-textconv "${salvage_base}"..HEAD --stdout >"$tmp" 2>/dev/null || true
    fi
    if [ ! -s "$tmp" ]; then
        [ -n "$git_src" ] && git -C "$git_src" diff --binary HEAD >"$tmp" 2>/dev/null || true
    fi
    if [ ! -s "$tmp" ]; then
        [ -n "$git_src" ] && git -C "$git_src" diff --binary --cached >"$tmp" 2>/dev/null || true
    fi
    if [ -s "$tmp" ]; then
        mv -f "$tmp" "$dest" || rm -f "$tmp"
    else
        rm -f "$tmp"
    fi
    return 0
}

while [ $# -gt 0 ]; do
    case "$1" in
        --manifest) manifest="${2:-}"; shift 2 ;;
        --out-dir) out_dir="${2:-}"; shift 2 ;;
        --lane-timeout) lane_timeout_override="${2:-}"; shift 2 ;;
        --token-budget) token_budget="${2:-}"; shift 2 ;;
        -h | --help) usage ;;
        *) die "unknown argument: $1" ;;
    esac
done

[ -n "$manifest" ] || die "--manifest required"
[ -n "$out_dir" ] || die "--out-dir required"
[ -f "$manifest" ] || die "manifest not found: $manifest"
if [ -n "$token_budget" ]; then
    case "$token_budget" in *[!0-9]* | "") die "--token-budget must be a non-negative integer" ;; esac
fi
if [ -n "$lane_timeout_override" ]; then
    case "$lane_timeout_override" in *[!0-9]* | "") die "--lane-timeout must be a non-negative integer" ;; esac
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
remote_agent="${REMOTE_AGENT:-${script_dir}/remote_agent.sh}"
python="${repo_root}/.venv/bin/python"
backend_spec_py="${repo_root}/packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_spec.py"
lane_result_py="${repo_root}/packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/lane_result.py"
review_runner_py="${repo_root}/packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/review_runner.py"

# Receipt starts as soon as out_dir is known so validation aborts still write D8.
mkdir -p "$out_dir"
started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
receipt_lanes_ndjson="${out_dir}/.receipt-lanes.ndjson"
: >"$receipt_lanes_ndjson"
trap '_write_receipt' EXIT

if [ ! -x "$python" ]; then
    die "interpreter not found: ${python} (required to parse manifests and build specs)"
fi

# Resolve canonical backend/effort once (Python owns defaults; bash has none).
flock_default_backend=""
flock_default_effort=""
if ! _defaults_out="$("$python" "$backend_spec_py" defaults)"; then
    die "could not obtain flock defaults from backend_spec"
fi
while IFS= read -r _dline || [ -n "${_dline:-}" ]; do
    case "${_dline}" in
        backend=* | effort=*)
            # shellcheck disable=SC2086
            eval "flock_default_${_dline}"
            ;;
    esac
done <<<"$_defaults_out"
if [ -z "${flock_default_backend:-}" ] || [ -z "${flock_default_effort:-}" ]; then
    die "could not obtain flock defaults from backend_spec (empty backend or effort)"
fi

declare -a lane_ids=() kinds=() briefs=() branches=() schema_overrides=()
declare -a backends=() models=() efforts=() est_tokens_list=() speeds=()
declare -a lane_errors=()

# Phase one: read every line and record a per-line validation verdict.
# Lane-level errors do not die; structural failures still die.
line_no=0
while IFS= read -r _manifest_line || [ -n "${_manifest_line:-}" ]; do
    line_no=$((line_no + 1))
    if [ -z "${_manifest_line//[[:space:]]/}" ]; then
        continue
    fi
    lane_id="" kind="" brief_path="" branch="" schema_path="" backend="" model="" effort="" est_tokens="" speed=""
    # shellcheck disable=SC1090
    if ! eval "$(
        "$python" -c '
import shlex, sys
raw = sys.stdin.read().rstrip("\r\n")
parts = [p.rstrip("\r") for p in raw.split("\t")]
if len(parts) > 10:
    sys.stderr.write("too many columns\n")
    sys.exit(2)
parts += [""] * (10 - len(parts))
names = [
    "lane_id", "kind", "brief_path", "branch", "schema_path",
    "backend", "model", "effort", "est_tokens", "speed",
]
for name, val in zip(names, parts[:10]):
    print(f"{name}={shlex.quote(val)}")
' <<<"$_manifest_line"
    )"; then
        _append_synthetic_not_dispatched "manifest line ${line_no}: bad columns"
        die "manifest line ${line_no}: bad columns"
    fi

    # Empty backend/effort columns: use cached Python defaults (explicit wins).
    if [ -z "${backend:-}" ]; then
        backend="$flock_default_backend"
    fi
    if [ -z "${effort:-}" ]; then
        effort="$flock_default_effort"
    fi

    err=""
    if [ -z "${lane_id:-}" ] || [ -z "${kind:-}" ] || [ -z "${brief_path:-}" ] || [ -z "${branch:-}" ]; then
        err="manifest line ${line_no}: expected lane_id kind brief_path branch ..."
    else
        case "$kind" in
            review | edit) ;;
            *) err="unknown kind: ${kind} (lane ${lane_id})" ;;
        esac
        if [ -z "$err" ] && [ ! -f "$brief_path" ]; then
            err="brief path does not exist: ${brief_path} (lane ${lane_id})"
        fi
        if [ -z "$err" ] && [ -n "${schema_path:-}" ] && [ ! -f "$schema_path" ]; then
            err="schema path does not exist: ${schema_path} (lane ${lane_id})"
        fi
        if [ -z "$err" ] && [ -n "${est_tokens:-}" ]; then
            case "$est_tokens" in
                *[!0-9]*) err="est_tokens must be a non-negative integer (lane ${lane_id})" ;;
            esac
        fi
    fi

    lane_ids+=("$lane_id")
    kinds+=("$kind")
    briefs+=("$brief_path")
    branches+=("$branch")
    schema_overrides+=("${schema_path:-}")
    backends+=("$backend")
    models+=("${model:-}")
    efforts+=("${effort:-}")
    est_tokens_list+=("${est_tokens:-}")
    speeds+=("${speed:-}")
    lane_errors+=("$err")
done <"$manifest"

[ "${#lane_ids[@]}" -gt 0 ] || die "manifest is empty: $manifest"

for i in "${!lane_ids[@]}"; do
    b="${branches[$i]}"
    for j in "${!lane_ids[@]}"; do
        [ "$j" -ge "$i" ] && break
        if [ "${branches[$j]}" = "$b" ]; then
            _append_synthetic_not_dispatched "duplicate branch: ${b} (lanes ${lane_ids[$j]} and ${lane_ids[$i]})"
            die "duplicate branch: ${b} (lanes ${lane_ids[$j]} and ${lane_ids[$i]})"
        fi
    done
done

# Distinct lane_ids that collapse to one filename slug (a/b vs a_b) would
# silently overwrite one artifact triple. Refuse before dispatch.
for i in "${!lane_ids[@]}"; do
    slug="$(_lane_slug "${lane_ids[$i]}")"
    for j in "${!lane_ids[@]}"; do
        [ "$j" -ge "$i" ] && break
        other_slug="$(_lane_slug "${lane_ids[$j]}")"
        if [ "$other_slug" = "$slug" ] && [ "${lane_ids[$j]}" != "${lane_ids[$i]}" ]; then
            _append_synthetic_not_dispatched "slug collision: ${slug} (lanes ${lane_ids[$j]} and ${lane_ids[$i]})"
            die "slug collision: ${slug} (lanes ${lane_ids[$j]} and ${lane_ids[$i]})"
        fi
    done
done

# If any lane-level validation failed, write a receipt row for every line and abort.
has_lane_errors=0
for i in "${!lane_ids[@]}"; do
    if [ -n "${lane_errors[$i]}" ]; then
        has_lane_errors=1
        echo "offload_flock: ${lane_errors[$i]}" >&2
    fi
done
if [ "$has_lane_errors" -eq 1 ]; then
    for i in "${!lane_ids[@]}"; do
        lane_id="${lane_ids[$i]}"
        kind="${kinds[$i]}"
        backend="${backends[$i]}"
        model="${models[$i]}"
        effort="${efforts[$i]}"
        if [ -n "${lane_errors[$i]}" ]; then
            reason="${lane_errors[$i]}"
        else
            reason="flock aborted before dispatch (another lane failed validation)"
        fi
        _append_lane_receipt "$(
            "$python" -c '
import json, sys
print(json.dumps({
  "lane_id": sys.argv[1], "kind": sys.argv[2], "backend_id": sys.argv[3],
  "model": sys.argv[4], "effort": sys.argv[5], "service_tier": None, "lane_timeout_s": 0,
  "state": "not_dispatched", "dispatched_at": None, "finished_at": None,
  "agent_exit_code": None, "reason": sys.argv[6],
  "est_tokens": 0, "observed_tokens": None,
  "token_provenance": None, "charged_tokens": None,
  "estimate_overshoot": False, "result_parse": None, "operator_action": None,
}))
' "${lane_id:-}" "${kind:-}" "${backend:-}" "${model:-}" "${effort:-}" "$reason"
        )"
    done
    aborted="validation"
    flock_exit=2
    exit 2
fi

default_edit_schema="${out_dir}/edit-result.schema.json"
default_review_schema="${out_dir}/review-output.schema.json"
if ! "$python" "$lane_result_py" schema >"$default_edit_schema"; then
    die "failed to materialize edit schema"
fi
if ! "$python" "$review_runner_py" schema >"$default_review_schema"; then
    die "failed to materialize review schema"
fi
if ! "$python" "$backend_spec_py" schema-receipt >"${out_dir}/flock-receipt.schema.json"; then
    die "failed to materialize receipt schema"
fi

# Resolve est_tokens (column or DEFAULT_EST_TOKENS).
declare -a resolved_est=()
for i in "${!lane_ids[@]}"; do
    lane_id="${lane_ids[$i]}"
    backend="${backends[$i]}"
    raw_est="${est_tokens_list[$i]}"
    if [ -n "$raw_est" ]; then
        est="$raw_est"
    else
        est="$("$python" "$backend_spec_py" default-est --backend "$backend")" || die "no DEFAULT_EST_TOKENS for lane ${lane_id} (backend ${backend})"
    fi
    if [ -n "$token_budget" ] && [ "${est:-0}" -le 0 ]; then
        die "lane ${lane_id}: est_tokens must be positive when --token-budget is set (got ${est})"
    fi
    resolved_est+=("$est")
done

declare -a agent_specs=() lane_timeouts=() encoded_models=() encoded_efforts=() encoded_service_tiers=()
for i in "${!lane_ids[@]}"; do
    lane_id="${lane_ids[$i]}"
    backend="${backends[$i]}"
    model="${models[$i]}"
    effort="${efforts[$i]}"
    speed="${speeds[$i]}"
    spec_base="${out_dir}/$(_lane_slug "$lane_id").agent.spec"
    build_args=("$python" "$backend_spec_py" build --backend "$backend" --out "$spec_base")
    if [ -n "$effort" ]; then
        build_args+=(--effort "$effort")
    fi
    if [ -n "$model" ]; then
        build_args+=(--model "$model")
    fi
    if [ -n "$speed" ]; then
        build_args+=(--speed "$speed")
    fi
    # Always pass brief contents; some backends bake them into positional argv.
    # Other recipes ignore prompt (stdin/brief-file are separate).
    build_args+=(--prompt-file "${briefs[$i]}")
    build_err="$(mktemp "${TMPDIR:-/tmp}/offload_flock_spec.XXXXXX")"
    if ! "${build_args[@]}" >"${spec_base}.build.log" 2>"$build_err"; then
        err_body="$(cat "$build_err" 2>/dev/null || true)"
        rm -f "$build_err"
        die "could not build agent spec for lane ${lane_id} (backend ${backend}): ${err_body}"
    fi
    rm -f "$build_err"
    [ -f "${spec_base}.json" ] || die "agent spec missing for lane ${lane_id}"
    agent_specs+=("${spec_base}.json")
    # shellcheck disable=SC2034
    eval "$(
        "$python" -c '
import json, shlex, sys
spec = json.load(open(sys.argv[1], encoding="utf-8"))
ops = spec.get("operator_values") or []
print("lane_timeout_s=" + shlex.quote(str(int(spec.get("lane_timeout_s") or 0))))
print("encoded_model=" + shlex.quote(str(ops[0] if len(ops) > 0 else "")))
print("encoded_effort=" + shlex.quote(str(ops[1] if len(ops) > 1 else "")))
print("encoded_service_tier=" + shlex.quote(str(ops[2] if len(ops) > 2 else "")))
' "${spec_base}.json"
    )"
    if [ -n "$lane_timeout_override" ]; then
        lane_timeouts+=("$lane_timeout_override")
    else
        lane_timeouts+=("$lane_timeout_s")
    fi
    encoded_models+=("$encoded_model")
    encoded_efforts+=("$encoded_effort")
    encoded_service_tiers+=("$encoded_service_tier")
done

declare -a summary_lines=()
DISPATCH_AGENT_RC=0

dispatch_lane() {
    local lane_id="$1" kind="$2" brief_path="$3" branch="$4" schema="$5" agent_spec="$6" timeout_s="$7"
    local lane_slug
    lane_slug="$(_lane_slug "$lane_id")"
    local result_out="${out_dir}/${lane_slug}.result.json"
    local patch_out="${out_dir}/${lane_slug}.patch"
    local salvage_git=""
    # Host salvage is best-effort and only against a source that actually
    # exists. Do not invent ${lane_slug}.sandbox — nothing creates that path.
    # The reliable salvage is remote-side (remote_agent.sh exit-3 committed
    # format-patch / exit-4 uncommitted diff) before sandbox teardown.
    if [ -n "${OFFLOAD_FLOCK_SALVAGE_GIT:-}" ] && [ -d "${OFFLOAD_FLOCK_SALVAGE_GIT}" ]; then
        salvage_git="$OFFLOAD_FLOCK_SALVAGE_GIT"
    fi
    local -a cmd
    local rc=0
    DISPATCH_AGENT_RC=0

    cmd=(
        "$remote_agent" build
        --branch "$branch"
        --brief "$brief_path"
        --schema "$schema"
        --result-out "$result_out"
        --agent-spec "$agent_spec"
    )
    if [ -n "$timeout_s" ] && [ "$timeout_s" -gt 0 ]; then
        cmd+=(--timeout "$timeout_s")
    fi
    if [ "$kind" = "edit" ]; then
        cmd+=(--out "$patch_out")
    fi

    rc=0
    "${cmd[@]}" || rc=$?
    DISPATCH_AGENT_RC=$rc

    if [ "$kind" = "review" ]; then
        if [ "$rc" -eq 0 ] || [ "$rc" -eq 4 ]; then
            summary_lines+=("${lane_id}: ok (review, exit ${rc})")
            return 0
        fi
        echo "offload_flock: lane ${lane_id}: failed (review, exit ${rc})" >&2
        summary_lines+=("${lane_id}: FAIL (review, exit ${rc})")
        return 1
    fi
    if [ "$rc" -eq 3 ] || [ "$rc" -eq 4 ]; then
        # Exit-3 already format-patches committed work onto --out; give exit 4
        # the same host-side salvage arm when a real git_src exists. Do not
        # change exit-code semantics: both remain failures for edit lanes.
        if [ ! -s "$patch_out" ]; then
            _salvage_edit_patch "$patch_out" "$salvage_git" "${OFFLOAD_FLOCK_SALVAGE_BASE:-}"
            if [ -s "$patch_out" ]; then
                echo "offload_flock: lane ${lane_id}: salvage patch -> ${patch_out}" >&2
            fi
        fi
    fi
    if [ "$rc" -eq 4 ]; then
        echo "offload_flock: lane ${lane_id}: failed — no committed changes (exit 4)" >&2
        summary_lines+=("${lane_id}: FAIL (edit, no committed changes)")
        return 1
    fi
    # Exit 5 = result present but unparseable. That says nothing about the patch:
    # when a non-empty patch exists, treat the lane as success with degraded parse
    # (DISPATCH_AGENT_RC stays 5 so the caller can set result_parse=degraded).
    if [ "$rc" -eq 5 ] && [ -s "$patch_out" ]; then
        summary_lines+=("${lane_id}: ok (edit, result_parse=degraded)")
        return 0
    fi
    if [ "$rc" -ne 0 ]; then
        echo "offload_flock: lane ${lane_id}: failed (edit, exit ${rc})" >&2
        summary_lines+=("${lane_id}: FAIL (edit, exit ${rc})")
        return 1
    fi
    if [ ! -s "$patch_out" ]; then
        echo "offload_flock: lane ${lane_id}: empty patch / no patch" >&2
        summary_lines+=("${lane_id}: FAIL (empty patch / no patch)")
        DISPATCH_AGENT_RC=1
        return 1
    fi
    summary_lines+=("${lane_id}: ok (edit)")
    return 0
}

for i in "${!lane_ids[@]}"; do
    lane_id="${lane_ids[$i]}"
    kind="${kinds[$i]}"
    backend="${backends[$i]}"
    est="${resolved_est[$i]}"
    model_enc="${encoded_models[$i]}"
    effort_enc="${encoded_efforts[$i]}"
    service_tier_enc="${encoded_service_tiers[$i]}"
    timeout_s="${lane_timeouts[$i]}"
    schema="${schema_overrides[$i]}"
    if [ -z "$schema" ]; then
        if [ "$kind" = "edit" ]; then
            schema="$default_edit_schema"
        else
            schema="$default_review_schema"
        fi
    fi

    if [ "$refuse_rest" -eq 1 ]; then
        _append_lane_receipt "$(
            "$python" -c '
import json, sys
print(json.dumps({
  "lane_id": sys.argv[1], "kind": sys.argv[2], "backend_id": sys.argv[3],
  "model": sys.argv[4], "effort": sys.argv[5], "service_tier": sys.argv[8] or None,
  "lane_timeout_s": int(sys.argv[6]),
  "state": "not_dispatched", "dispatched_at": None, "finished_at": None,
  "agent_exit_code": None, "reason": "flock aborted before dispatch",
  "est_tokens": int(sys.argv[7]), "observed_tokens": None,
  "token_provenance": None, "charged_tokens": None,
  "estimate_overshoot": False, "result_parse": None, "operator_action": None,
}))
' "$lane_id" "$kind" "$backend" "$model_enc" "$effort_enc" "$timeout_s" "$est" "$service_tier_enc"
        )"
        continue
    fi

    if [ -n "$token_budget" ] && [ $((charged_total + est)) -gt "$token_budget" ]; then
        echo "offload_flock: lane ${lane_id}: refused_budget (charged=${charged_total} est=${est} budget=${token_budget})" >&2
        aborted="budget_breach"
        refuse_rest=1
        any_failed=1
        _append_lane_receipt "$(
            "$python" -c '
import json, sys
print(json.dumps({
  "lane_id": sys.argv[1], "kind": sys.argv[2], "backend_id": sys.argv[3],
  "model": sys.argv[4], "effort": sys.argv[5], "service_tier": sys.argv[10] or None,
  "lane_timeout_s": int(sys.argv[6]),
  "state": "refused_budget", "dispatched_at": None, "finished_at": None,
  "agent_exit_code": None,
  "reason": f"pre-charge refuse: charged={sys.argv[8]} est={sys.argv[7]} budget={sys.argv[9]}",
  "est_tokens": int(sys.argv[7]), "observed_tokens": None,
  "token_provenance": None, "charged_tokens": None,
  "estimate_overshoot": False, "result_parse": None, "operator_action": None,
}))
' "$lane_id" "$kind" "$backend" "$model_enc" "$effort_enc" "$timeout_s" "$est" "$charged_total" "$token_budget" "$service_tier_enc"
        )"
        # Mark remaining as not_dispatched on subsequent iterations via refuse_rest.
        continue
    fi

    dispatched_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    if ! dispatch_lane "$lane_id" "$kind" "${briefs[$i]}" "${branches[$i]}" "$schema" "${agent_specs[$i]}" "$timeout_s"; then
        any_failed=1
        finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        lane_state="failed"
        lane_parse="ok"
        lane_reason="lane failed (exit ${DISPATCH_AGENT_RC})"
        receipt_service_tier="$service_tier_enc"
        unconfirmed_service_tier=""
        charge_est=1
        case "$DISPATCH_AGENT_RC" in
            2)
                lane_reason="usage or validation error"
                ;;
            3)
                lane_reason="agent run failed"
                ;;
            4)
                lane_reason="no committed changes"
                ;;
            5)
                lane_state="degraded"
                lane_parse="degraded"
                lane_reason="result_unparseable"
                ;;
            6)
                lane_reason="auth failure"
                ;;
            7)
                lane_state="refused_bound"
                lane_reason="no process bound obtainable (policy refusal)"
                ;;
            8)
                # remote_agent classifies wall-clock bound expiry and reports
                # exit 8 (deadline reached and/or timeout(1) status 124). Keep
                # state=failed; name the expiry so operators distinguish it
                # from an ordinary crash.
                lane_reason="wall-clock bound expired (exit 8)"
                ;;
            10)
                lane_reason="requested service tier unconfirmed by provider"
                receipt_service_tier=""
                unconfirmed_service_tier="$service_tier_enc"
                ;;
            75)
                lane_state="not_dispatched"
                lane_parse="null"
                lane_reason="retryable defer: VM memory floor, lane cap, residual timeout exhausted, or same-branch lane already active"
                charge_est=0
                ;;
            78)
                lane_reason="host not configured"
                refuse_rest=1
                ;;
        esac
        if [ "$charge_est" -eq 1 ]; then
            charged_total=$((charged_total + est))
        fi
        _append_lane_receipt "$(
            "$python" -c '
import json, sys
charge = sys.argv[14] == "1"
rp = sys.argv[13]
result_parse = None if rp == "null" else rp
print(json.dumps({
  "lane_id": sys.argv[1], "kind": sys.argv[2], "backend_id": sys.argv[3],
  "model": sys.argv[4], "effort": sys.argv[5], "service_tier": sys.argv[15] or None,
  "requested_service_tier": sys.argv[16] or None,
  "service_tier_confirmation": "unconfirmed" if sys.argv[16] else None,
  "lane_timeout_s": int(sys.argv[6]),
  "state": sys.argv[11], "dispatched_at": sys.argv[8], "finished_at": sys.argv[9],
  "agent_exit_code": int(sys.argv[10]), "reason": sys.argv[12],
  "est_tokens": int(sys.argv[7]), "observed_tokens": None,
  "token_provenance": "estimated" if charge else None,
  "charged_tokens": int(sys.argv[7]) if charge else None,
  "estimate_overshoot": False, "result_parse": result_parse, "operator_action": None,
}))
' "$lane_id" "$kind" "$backend" "$model_enc" "$effort_enc" "$timeout_s" "$est" "$dispatched_at" "$finished_at" "$DISPATCH_AGENT_RC" "$lane_state" "$lane_reason" "$lane_parse" "$charge_est" "$receipt_service_tier" "$unconfirmed_service_tier"
        )"
        if [ "$DISPATCH_AGENT_RC" -eq 6 ]; then
            aborted="auth_failure"
            refuse_rest=1
        fi
        continue
    fi
    finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    # No stream parser in hermetic path yet — charge estimate (token_provenance=estimated).
    charged_total=$((charged_total + est))
    # DISPATCH_AGENT_RC=5 on a returned-0 edit lane means result stream was
    # unparseable but a non-empty patch was produced (GATE-M01 degraded success).
    success_parse="ok"
    success_reason=""
    if [ "$DISPATCH_AGENT_RC" -eq 5 ]; then
        success_parse="degraded"
        success_reason="result_unparseable"
    fi
    _append_lane_receipt "$(
        "$python" -c '
import json, sys
print(json.dumps({
  "lane_id": sys.argv[1], "kind": sys.argv[2], "backend_id": sys.argv[3],
  "model": sys.argv[4], "effort": sys.argv[5], "service_tier": sys.argv[13] or None,
  "lane_timeout_s": int(sys.argv[6]),
  "state": "ok", "dispatched_at": sys.argv[8], "finished_at": sys.argv[9],
  "agent_exit_code": int(sys.argv[10]), "reason": sys.argv[12],
  "est_tokens": int(sys.argv[7]), "observed_tokens": None,
  "token_provenance": "estimated", "charged_tokens": int(sys.argv[7]),
  "estimate_overshoot": False, "result_parse": sys.argv[11], "operator_action": None,
}))
' "$lane_id" "$kind" "$backend" "$model_enc" "$effort_enc" "$timeout_s" "$est" "$dispatched_at" "$finished_at" "$DISPATCH_AGENT_RC" "$success_parse" "$success_reason" "$service_tier_enc"
    )"
    if [ -n "$token_budget" ] && [ "$charged_total" -gt "$token_budget" ]; then
        aborted="budget_breach"
        refuse_rest=1
        any_failed=1
    fi
done

echo "offload_flock: summary"
if [ "${#summary_lines[@]}" -gt 0 ]; then
    for line in "${summary_lines[@]}"; do
        echo "  ${line}"
    done
fi

if [ -n "$aborted" ]; then
    flock_exit=2
    exit 2
fi
if [ "$any_failed" -ne 0 ]; then
    flock_exit=1
    exit 1
fi
flock_exit=0
exit 0
