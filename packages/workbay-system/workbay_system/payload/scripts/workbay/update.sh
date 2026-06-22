#!/bin/sh
# One-shot workbay stack update (internal).
#
# Deterministic consumer updater — no LLM, no version archeology:
#   1. locate the Python runtime that owns the workbay-bootstrap console
#      script (its shebang), and pick one explicit upgrade adapter;
#   2. upgrade the workbay-stack meta-package in THAT runtime (one version
#      number moves the whole stack);
#   3. refresh the overlay: `workbay-bootstrap update --target <repo>`
#      (package source needs no ref; git_overlay takes REMOTE_REF=<tag> or
#      prints migration guidance and exits 2);
#   4. `workbay-bootstrap doctor`;
#   5. print the stack version table and exit with doctor's code.
#
# Usage: update.sh [TARGET]            (TARGET defaults to the git toplevel)
#   REMOTE_REF=<tag>           git_overlay consumers: ref to update to
#   WORKBAY_UPDATE_DRY_RUN=1 print each mutating step instead of running it
#
# This file is a managed payload surface: it self-updates with the stack.

set -eu

DRY_RUN="${WORKBAY_UPDATE_DRY_RUN:-}"
STACK_DIST="workbay-stack"

log() { printf 'workbay-update: %s\n' "$*"; }
fail() { printf 'workbay-update: ERROR: %s\n' "$*" >&2; exit 1; }

run() {
    # Print every mutating command; execute unless dry-run.
    printf 'workbay-update: + %s\n' "$*"
    if [ -z "$DRY_RUN" ]; then
        "$@"
    fi
}

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    TARGET="$(git rev-parse --show-toplevel 2>/dev/null)" ||
        fail "no TARGET given and not inside a git repository"
fi
[ -d "$TARGET" ] || fail "target directory does not exist: $TARGET"

MANIFEST="$TARGET/.workbay-bootstrap.json"
[ -f "$MANIFEST" ] || fail "no .workbay-bootstrap.json in $TARGET — run workbay-bootstrap install first"

# --- 1. resolve the runtime owning workbay-bootstrap --------------------
BOOTSTRAP_BIN="$(command -v workbay-bootstrap)" ||
    fail "workbay-bootstrap not on PATH — install the workbay stack first (pip install workbay-stack)"

# Console-script shebang names the owning interpreter; `#!/usr/bin/env X`
# resolves through PATH.
SHEBANG="$(head -n 1 "$BOOTSTRAP_BIN")"
RUNTIME_PY="${SHEBANG#\#!}"
case "$RUNTIME_PY" in
    *"/env "*)
        RUNTIME_PY="$(command -v "${RUNTIME_PY##*/env }")" ||
            fail "cannot resolve interpreter from shebang: $SHEBANG"
        ;;
esac
# Trim a trailing argument-free spaces form like "/usr/bin/python3 -s".
RUNTIME_PY="${RUNTIME_PY%% *}"
[ -x "$RUNTIME_PY" ] ||
    fail "runtime interpreter for workbay-bootstrap is not executable: $RUNTIME_PY (from $BOOTSTRAP_BIN)"
log "runtime: $RUNTIME_PY (owns $BOOTSTRAP_BIN)"

# --- 2. upgrade the stack anchor in that runtime ---------------------------
case "$BOOTSTRAP_BIN" in
    *"/pipx/venvs/"*)
        command -v pipx >/dev/null 2>&1 ||
            fail "workbay-bootstrap is pipx-owned but pipx is not on PATH"
        log "adapter: pipx inject"
        run pipx inject --include-apps workbay-bootstrap "$STACK_DIST" \
            --pip-args "--upgrade"
        ;;
    *)
        if command -v uv >/dev/null 2>&1; then
            log "adapter: uv pip (runtime-pinned)"
            run uv pip install --python "$RUNTIME_PY" --upgrade "$STACK_DIST"
        else
            log "adapter: python -m pip (runtime-pinned)"
            run "$RUNTIME_PY" -m pip install --upgrade "$STACK_DIST"
        fi
        ;;
esac

# Verify the anchor and its exact member pins resolve in the SAME runtime before
# touching the repo.
if [ -z "$DRY_RUN" ]; then
    "$RUNTIME_PY" - "$STACK_DIST" <<'PY'
import importlib.metadata as metadata
import re
import sys

stack_dist = sys.argv[1]
pin_re = re.compile(r"^\s*([A-Za-z0-9._-]+)\s*==\s*([^\s;]+)\s*$")

try:
    stack_version = metadata.version(stack_dist)
except metadata.PackageNotFoundError:
    raise SystemExit(
        f"{stack_dist} did not install into this runtime; adapter mismatch"
    )

pins = []
for requirement in metadata.requires(stack_dist) or []:
    match = pin_re.match(requirement)
    if match:
        pins.append(match.groups())

if not pins:
    raise SystemExit(f"{stack_dist} metadata has no exact member pins")

errors = []
for distribution, expected in pins:
    try:
        installed = metadata.version(distribution)
    except metadata.PackageNotFoundError:
        errors.append(f"{distribution} not installed (requires =={expected})")
        continue
    if installed != expected:
        errors.append(f"{distribution} installed {installed} != required {expected}")

if errors:
    raise SystemExit(
        "stack member verification failed:\n  " + "\n  ".join(errors)
    )

print(
    f"workbay-update: verified {stack_dist}=={stack_version} "
    f"with {len(pins)} pinned member(s)"
)
PY
fi

# --- 3. refresh the overlay from its recorded source -----------------------
SOURCE_KIND="$("$RUNTIME_PY" -c "
import json, sys
print(json.load(open(sys.argv[1])).get('source_kind') or 'git_overlay')
" "$MANIFEST")"

if [ "$SOURCE_KIND" = "package" ]; then
    run "$BOOTSTRAP_BIN" update --target "$TARGET"
elif [ -n "${REMOTE_REF:-}" ]; then
    run "$BOOTSTRAP_BIN" update --target "$TARGET" --remote-ref "$REMOTE_REF"
else
    log "this overlay is git_overlay-sourced; pass REMOTE_REF=<tag> to update it,"
    log "or migrate to the package source (recommended):"
    log "    workbay-bootstrap install --source package --target $TARGET"
    exit 2
fi

# --- 4 + 5. doctor, then the version table ---------------------------------
DOCTOR_STATUS=0
run "$BOOTSTRAP_BIN" doctor --target "$TARGET" || DOCTOR_STATUS=$?

"$RUNTIME_PY" -c "
import importlib.metadata as metadata
import json
import sys

manifest = json.load(open(sys.argv[1]))
rows = [('workbay-system', manifest.get('package_version'))]
if manifest.get('stack_distribution'):
    rows.append((manifest['stack_distribution'], manifest.get('stack_version')))
    rows.extend(sorted((manifest.get('stack_members') or {}).items()))
print('workbay-update: stack versions (installed / manifest):')
seen = set()
for dist, recorded in rows:
    if dist in seen:
        continue
    seen.add(dist)
    try:
        installed = metadata.version(dist)
    except metadata.PackageNotFoundError:
        installed = 'NOT INSTALLED'
    marker = '' if installed == (recorded or installed) else '  <-- drift'
    print(f'  {dist:32} {installed:12} / {recorded or \"-\"}{marker}')
" "$MANIFEST" || log "version table unavailable (manifest unreadable)"
# ^ informational only — must not let set -e preempt the doctor exit code.

exit "$DOCTOR_STATUS"
