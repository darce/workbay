#!/usr/bin/env bash
# Failure-id capture with a FORCED-CONSISTENT CHECKOUT and CHECKOUT-STABLE IDS.
#   usage: gate_ids.sh <worktree> <out-prefix> <pytest targets...>
#
# Two measurement defects this encodes, both observed on real lane gates:
#
# 1. CONSISTENT CHECKOUT. The lane .venv symlinks to the root .venv whose
#    zz_dev_redirect_*.pth points at the ROOT checkout, so a bare run imports
#    main's packages while executing the branch's tests. Any branch that ADDS a
#    symbol then manufactures phantom ImportError "regressions" (29 of them, on
#    lc-ic1). The PYTHONPATH prefix is the fix.
#
# 2. CHECKOUT-STABLE IDS. Some tests are parameterized on the repo DIRECTORY
#    NAME, so the same failure carries a different id in every checkout and the
#    diff reports one phantom NEW plus one phantom FIXED per such test. Ids are
#    written twice: <prefix>.ids raw, <prefix>.nids with the checkout token
#    replaced by [CHECKOUT]. Diff the .nids; keep the .ids for forensics.
# `set -u` only, deliberately: `-e` would abort on a green pytest (nonzero
# from the suite, not from this producer), and `-o pipefail` would make the
# tee'd pytest pipeline's nonzero status the script's status. This tool's
# result is the artifacts it writes, not pytest's exit code. Failure ids are
# extracted by landing_gate.py --extract-ids, not by grepping ERROR lines.
set -u
# Two failure modes this guard exists for, both hit on 2026-09-07:
#   * bash reads a script incrementally by BYTE OFFSET. Editing this file while
#     an instance is running desyncs it and the tail of the script becomes a
#     syntax error AFTER the 37-minute pytest run has already completed, losing
#     the whole measurement. Runs execute from an immutable per-run copy.
#   * two runs sharing an out-prefix both open <prefix>.raw with O_TRUNC and
#     interleave, producing a raw log that looks plausible and is unparseable.
#     The prefix is claimed exclusively for the life of the run.
W="$(cd "$1" && pwd)"; OUT="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2
# OUT is absolutized BEFORE the cd below: a relative prefix would otherwise
# write the run's artifacts into the subject worktree and dirty the tree the
# gate is measuring.
# Executed wrapper identity is this process's script, which may be an
# immutable /tmp snapshot rather than a selected sibling named gate_ids.sh
# next to landing_gate.py [LANDGA-S1C-M04].
_self="${BASH_SOURCE[0]:-$0}"
if [ -e "$_self" ]; then
  WRAPPER="$(cd "$(dirname "$_self")" && pwd)/$(basename "$_self")"
else
  WRAPPER="$_self"
fi
# Attempt identity is claimed BEFORE preflight. A rejected interpreter, gate
# script, or PYTHONPATH probe must not leave a prior complete raw/ids/scope
# receipt set in place for a downstream consumer [REVIEW-H-03]. The lock is
# kernel-held (flock): SIGKILL releases it, so a leftover lock file is not a
# permanent claim [REVIEW-M-08]. The file itself is not deleted on EXIT;
# deleting it would create a new inode and break exclusion.
LOCK="$OUT.lock"
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "gate_ids: out-prefix $OUT is already claimed — pick another prefix or wait for the live holder to release $LOCK" >&2
  exit 3
fi
# Publish the current-attempt marker immediately after lock/run setup and
# BEFORE invalidation. A crash in that window must not leave a prior complete
# evidence set mergeable: the consumer binds receipts to this marker
# [LANDGA-S1C-H02]. The marker is not deleted by invalidation.
if [ -r /proc/sys/kernel/random/uuid ]; then
  RUN_ID="$(tr -d '[:space:]' < /proc/sys/kernel/random/uuid)"
else
  RUN_ID="$$-$(date -u +%s)-$RANDOM"
fi
TIMEOUT_SEC="${GATE_IDS_TIMEOUT_SEC:-3600}"
cat > "$OUT.attempt" <<EOF
{
  "schema": "landing_measurement_attempt/v1",
  "status": "incomplete",
  "run_id": "${RUN_ID}",
  "pid": $$
}
EOF
if [ -n "${GATE_IDS_CRASH_AFTER_ATTEMPT:-}" ]; then
  echo "gate_ids: test crash after attempt marker (GATE_IDS_CRASH_AFTER_ATTEMPT)" >&2
  exit 4
fi
{
  printf 'pid=%s\n' "$$"
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'started=%s\n' "$(date -u +%s)"
} > "$LOCK"
_invalidate_success_artifacts() {
  rm -f "$OUT.ids" "$OUT.nids" "$OUT.scope" "$OUT.receipt" \
    "$OUT.ids.partial" "$OUT.nids.partial" "$OUT.scope.partial" "$OUT.receipt.partial"
}
_invalidate_prior_attempt() {
  _invalidate_success_artifacts
  rm -f "$OUT.raw" "$OUT.raw.partial" "$OUT.run.json"
}
_write_attempt_receipt() {
  _status="$1"
  _error="$2"
  _error="$(printf '%s' "$_error" | tr -d '\n\r"\\')"
  _receipt_timeout="$TIMEOUT_SEC"
  case "$_receipt_timeout" in
    ''|*[!0-9]*) _receipt_timeout=3600 ;;
  esac
  cat > "$OUT.receipt" <<EOF
{
  "schema": "landing_measurement_receipt/v1",
  "status": "${_status}",
  "pytest_returncode": null,
  "raw_sha256": null,
  "ids_sha256": null,
  "scope_sha256": null,
  "producer_fingerprint": "",
  "compat": "landing-measurement/v1",
  "producer_compat": "landing-measurement/v1",
  "timeout_seconds": ${_receipt_timeout},
  "run_id": "${RUN_ID}",
  "error": "${_error}"
}
EOF
}
_fail_preflight() {
  echo "$1" >&2
  _write_attempt_receipt incomplete "$1"
  exit 4
}
trap 'rm -f "$OUT.ids.partial" "$OUT.nids.partial" "$OUT.scope.partial" "$OUT.receipt.partial"; exec 9>&-' EXIT
_invalidate_prior_attempt
_write_attempt_receipt incomplete "attempt-started"
case "$TIMEOUT_SEC" in
  ''|*[!0-9]*)
    _fail_preflight "gate_ids: GATE_IDS_TIMEOUT_SEC must be a positive integer (got ${TIMEOUT_SEC})"
    ;;
esac
if [ "$TIMEOUT_SEC" -le 0 ]; then
  _fail_preflight "gate_ids: GATE_IDS_TIMEOUT_SEC must be a positive integer (got ${TIMEOUT_SEC})"
fi
# INTERPRETER. The ROOT checkout's .venv by design: the lane .venv symlinks to
# it, and the PYTHONPATH prefix below -- not the interpreter -- is what forces
# the checkout. Derived from the SUBJECT worktree, never from $0: gate_run.sh
# executes this script from an immutable /tmp snapshot, so $0 cannot locate the
# repo. `--git-common-dir` resolves a linked worktree to the main checkout's
# .git, whose parent is that root. Override with GATE_IDS_PYTHON.
if [ -n "${GATE_IDS_PYTHON:-}" ]; then
  PY="$GATE_IDS_PYTHON"
else
  _common_git="$(git -C "$W" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)" || _common_git=
  if [ -z "$_common_git" ]; then
    _fail_preflight "gate_ids: $W is not a git checkout and GATE_IDS_PYTHON is unset — refusing to guess an interpreter"
  fi
  PY="$(dirname "$_common_git")/.venv/bin/python"
fi
if [ ! -x "$PY" ]; then
  _fail_preflight "gate_ids: interpreter not executable: $PY — set GATE_IDS_PYTHON to the root checkout's venv python"
fi
# 3. ONE PRODUCER FOR THE IMPORT PATH. This list used to be hand-written here,
#    while scripts/landing_gate.py derived its own from glob("packages/*/src").
#    The two shared 3 of 7 entries. This copy omitted
#    packages/workbay-bootstrap/src, so every subject run in a lane worktree
#    died at "ERROR: workbay_bootstrap loaded from <root>, but cwd is <lane>"
#    before one test ran (reachB, 2026-09-07); the glob omitted
#    packages/workbay-system, whose tree is not under src/. The gate now emits
#    the canonical value and this script consumes it, so certifying an
#    environment and measuring under it are the same act (LANDGATE-PP-01).
GATE_PY="${LANDING_GATE_SCRIPT:-$W/scripts/landing_gate.py}"
if [ ! -f "$GATE_PY" ]; then
  _fail_preflight "gate_ids: no landing_gate.py at $GATE_PY — the import path has one producer; point LANDING_GATE_SCRIPT at a checkout that has it"
fi
MEASUREMENT_PY="$(cd "$(dirname "$GATE_PY")" && pwd)/landing_measurement.py"
if [ ! -f "$MEASUREMENT_PY" ]; then
  _fail_preflight "gate_ids: no landing_measurement.py at $MEASUREMENT_PY — producer identity and bounded pytest live next to landing_gate.py"
fi
PYTHONPATH="$("$PY" "$GATE_PY" --print-pythonpath --worktree "$W")" || {
  _fail_preflight "gate_ids: landing_gate.py --print-pythonpath failed for $W"; }
if [ -z "$PYTHONPATH" ]; then
  _fail_preflight "gate_ids: landing_gate.py --print-pythonpath produced an EMPTY path for $W — refusing rather than measuring an unforced checkout"
fi
export PYTHONPATH
BT="/tmp/gate-$(basename "$W")-$$"
cd "$W" || _fail_preflight "gate_ids: could not cd to $W"
STATUS_JSON="$OUT.run.json"
rm -f "$STATUS_JSON"
"$PY" "$MEASUREMENT_PY" --run-pytest --raw "$OUT.raw" --run-status "$STATUS_JSON" --cwd "$W" --timeout "$TIMEOUT_SEC" --run-id "$RUN_ID" -- \
    "$PY" -m pytest "$@" -p no:cacheprovider -q --basetemp="$BT" -p no:randomly
helper_rc=$?
if [ "$helper_rc" -ne 0 ]; then
  _invalidate_success_artifacts
  "$PY" "$MEASUREMENT_PY" --write-receipt "$OUT.receipt" --from-status "$STATUS_JSON" --raw "$OUT.raw" \
    --timeout "$TIMEOUT_SEC" --fingerprint-script "$GATE_PY" --fingerprint-wrapper "$WRAPPER" \
    --run-id "$RUN_ID" || true
  echo "gate_ids: bounded pytest did not return (helper_rc=$helper_rc) for $OUT.raw" >&2
  if [ -f "$OUT.raw" ]; then
    tail -3 "$OUT.raw"
  fi
  exit "$helper_rc"
fi
if [ -f "$OUT.raw" ]; then
  tail -3 "$OUT.raw"
fi
# Extract into a partial file so a failed producer cannot leave an empty
# success-shaped .ids that a caller could combine with a later .scope.
if ! "$PY" "$GATE_PY" --extract-ids "$OUT.raw" > "$OUT.ids.partial"; then
  echo "gate_ids: landing_gate.py --extract-ids failed for $OUT.raw" >&2
  _invalidate_success_artifacts
  _write_attempt_receipt incomplete "extract-ids failed"
  exit 4
fi
mv -f "$OUT.ids.partial" "$OUT.ids"
sort -u "$OUT.ids" -o "$OUT.ids"
if ! sed -E 's/\[workbay(-wb-[^]]*)?\]/[CHECKOUT]/g' "$OUT.ids" > "$OUT.nids.partial" \
  || ! sort -u "$OUT.nids.partial" -o "$OUT.nids.partial"; then
  echo "gate_ids: normalization failed for $OUT.nids" >&2
  _invalidate_success_artifacts
  _write_attempt_receipt incomplete "normalization failed"
  exit 4
fi
mv -f "$OUT.nids.partial" "$OUT.nids"
scope_cmd=("$PY" "$GATE_PY" --emit-scope "$OUT.scope.partial" --worktree "$W" --python "$PY")
scope_cmd+=(--producer-wrapper="$WRAPPER")
for _target in "$@"; do
  scope_cmd+=(--scope-target="$_target")
done
# Equals-form so argparse does not swallow dashed pytest flags as new options.
scope_cmd+=(--scope-option=-p --scope-option=no:cacheprovider --scope-option=-q)
scope_cmd+=(--scope-option="--basetemp=$BT" --scope-option=-p --scope-option=no:randomly)
if ! "${scope_cmd[@]}"; then
  echo "gate_ids: landing_gate.py --emit-scope failed for $OUT.scope" >&2
  _invalidate_success_artifacts
  _write_attempt_receipt incomplete "emit-scope failed"
  exit 4
fi
mv -f "$OUT.scope.partial" "$OUT.scope"
if ! "$PY" "$MEASUREMENT_PY" --write-receipt "$OUT.receipt.partial" --receipt-status complete \
    --from-status "$STATUS_JSON" --raw "$OUT.raw" --ids "$OUT.ids" --nids "$OUT.nids" \
    --scope "$OUT.scope" --timeout "$TIMEOUT_SEC" --fingerprint-script "$GATE_PY" \
    --fingerprint-wrapper "$WRAPPER" --run-id "$RUN_ID"
then
  echo "gate_ids: landing_measurement.py --write-receipt failed for $OUT.receipt" >&2
  _invalidate_success_artifacts
  _write_attempt_receipt incomplete "write-receipt failed"
  exit 4
fi
mv -f "$OUT.receipt.partial" "$OUT.receipt"
cat > "$OUT.attempt" <<EOF
{
  "schema": "landing_measurement_attempt/v1",
  "status": "complete",
  "run_id": "${RUN_ID}",
  "pid": $$
}
EOF
echo "raw=$(wc -l < "$OUT.ids") normalized=$(wc -l < "$OUT.nids") -> $OUT.nids"
