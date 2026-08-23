#!/usr/bin/env bash
# Provision / rotate / probe a remote backend's credential port on the gate VM
# (implementation note S3). The port (kind, env var, env file, binary, status argv, probe)
# comes from the orchestrator registry — this script holds NO table of its own.
#
# Usage:
#   ./scripts/provision_remote_auth.sh --backend <id> [--host gate@host]
#       [--key-file PATH] [--skip-smoke] [--env-already-written]
#
# Rotation IS re-running it (key never on any argv or in any child env):
#   WORKBAY_0XALPHA_API_KEY="$(security find-generic-password -s 0xalpha -a workbay -w)" \
#     ./scripts/provision_remote_auth.sh --backend 0xalpha-remote --host gate@<host>
#
# Key source order: process env $<ENV_VAR> -> --key-file -> no-echo prompt
# (TTY only). A device_login port exits 2 and names the VM login command.
# --env-already-written skips capture + write (the ~/.profile loader is still
# appended, idempotently); --skip-smoke skips the probe.
# Interpreter: $WORKBAY_PYTHON, else <repo>/.venv/bin/python, else
# `uv run --frozen --no-sync --project packages/mcp-workbay-orchestrator python`.
# The registry query (and git rev-parse) run under an allow-listed `env -i`,
# so the key never reaches the interpreter tree (uv, .pth hooks, ...).
#
# Docs: docs/runbooks/remote-gate-provisioning.md ("Credential port").
set -euo pipefail
# Never let an inherited xtrace (SHELLOPTS=xtrace / bash -x) echo the key.
set +x

BACKEND=""
BACKEND_SEEN=0
HOST=""
HOST_SEEN=0
KEY_FILE=""
SKIP_SMOKE=0
ENV_ALREADY_WRITTEN=0

usage() {
  sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --backend)
      if [ "$BACKEND_SEEN" -eq 1 ]; then
        echo "provision_remote_auth: repeated --backend (already '${BACKEND}')" >&2
        exit 2
      fi
      BACKEND="${2:-}"; BACKEND_SEEN=1; shift 2 ;;
    --host)
      # S3-L-07: an explicit empty --host "" is a typo, not a request for the
      # local $HOME; only an OMITTED --host means local (announced below).
      # S4-L-05: trim surrounding whitespace; a whitespace-only value is the
      # same typo as an empty one (ssh "" would read as the local host).
      HOST_RAW="${2:-}"
      HOST_TRIM="${HOST_RAW#"${HOST_RAW%%[![:space:]]*}"}"
      HOST_TRIM="${HOST_TRIM%"${HOST_TRIM##*[![:space:]]}"}"
      if [ $# -lt 2 ] || [ -z "$HOST_TRIM" ]; then
        echo "provision_remote_auth: --host given but empty; omit --host to provision the LOCAL \$HOME" >&2
        usage
      fi
      HOST="$HOST_TRIM"; HOST_SEEN=1; shift 2 ;;
    --key-file) KEY_FILE="${2:-}"; shift 2 ;;
    --skip-smoke) SKIP_SMOKE=1; shift ;;
    --env-already-written) ENV_ALREADY_WRITTEN=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [ -z "$BACKEND" ]; then
  echo "provision_remote_auth: --backend <id> is required" >&2
  usage
fi
if [ "$HOST_SEEN" -eq 0 ]; then
  echo "provision_remote_auth: no --host: provisioning LOCAL \$HOME" >&2
fi
# Backend ids are registry keys: reject anything else before it reaches argv
# of the interpreter (no whitespace, newlines, shell metacharacters).
if ! [[ "$BACKEND" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "provision_remote_auth: invalid backend id (want ^[a-z0-9][a-z0-9-]*\$)" >&2
  exit 2
fi

# --- locate the repo (symlink-safe; macOS has no readlink -f) ---------------
_self="${BASH_SOURCE[0]}"
while [ -L "$_self" ]; do
  _target="$(readlink "$_self")"
  case "$_target" in
    /*) _self="$_target" ;;
    *) _self="$(dirname "$_self")/${_target}" ;;
  esac
done
script_dir="$(cd "$(dirname "$_self")" && pwd)"

# Allow-listed clean environment for every child that is not a transport.
# `export -n` cannot run before the env var NAME is known (it comes from the
# registry), so the interpreter tree (git, uv, python, .pth hooks) would
# otherwise inherit the operator's key. Deny-by-default instead [WEB-33].
CLEAN_ENV=(env -i PATH="$PATH" HOME="$HOME" TMPDIR="${TMPDIR:-}")
for _v in LANG LC_ALL UV_CACHE_DIR UV_PROJECT_ENVIRONMENT VIRTUAL_ENV WORKBAY_PYTHON; do
  if [ -n "${!_v:-}" ]; then
    CLEAN_ENV+=("${_v}=${!_v}")
  fi
done

if ! repo_root="$("${CLEAN_ENV[@]}" git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null)"; then
  repo_root="$(cd "${script_dir}/.." && pwd)"
fi

if [ -n "${WORKBAY_PYTHON:-}" ]; then
  PY=("$WORKBAY_PYTHON")
elif [ -x "${repo_root}/.venv/bin/python" ]; then
  PY=("${repo_root}/.venv/bin/python")
elif command -v uv >/dev/null 2>&1; then
  PY=(uv run --frozen --no-sync --project "${repo_root}/packages/mcp-workbay-orchestrator" python)
else
  echo "provision_remote_auth: no interpreter (set WORKBAY_PYTHON, create ${repo_root}/.venv, or install uv)" >&2
  exit 2
fi

# --- registry query (one python call, line protocol, no eval) ---------------
# Lines: kind / env_var / env_file (VM-rendered) / binary (VM-rendered) /
# status_argv (display) / probe... . Paths come back already rendered by the
# registry (`$HOME/...` literal for the remote shell, or absolute) so bash
# never re-derives them.
PY_QUERY='
import sys
from workbay_orchestrator_mcp.orchestration.backend_registry import _vm_home_path, auth_port, render_auth_probe
port = auth_port(sys.argv[1])
print(port.kind)
print(port.env_var or "")
print(_vm_home_path(port.env_file) if port.env_file else "")
print(_vm_home_path(port.binary))
print(" ".join(port.status_argv))
print(render_auth_probe(port), end="")
'
_query_err="$(mktemp "${TMPDIR:-/tmp}/provision_remote_auth.XXXXXX")"
trap 'rm -f "$_query_err"' EXIT
if ! PORT_OUT="$("${CLEAN_ENV[@]}" "${PY[@]}" -c "$PY_QUERY" "$BACKEND" 2>"$_query_err")"; then
  cat "$_query_err" >&2
  echo "provision_remote_auth: registry lookup failed for backend '${BACKEND}'" >&2
  exit 2
fi
{
  IFS= read -r PORT_KIND
  IFS= read -r ENV_VAR
  IFS= read -r VM_ENVF
  IFS= read -r VM_BINARY
  IFS= read -r PORT_STATUS_ARGV
  PORT_PROBE="$(cat)"
} <<<"$PORT_OUT"

if [ "$PORT_KIND" != "env_file" ]; then
  echo "backend ${BACKEND} authenticates by device login; run \"${VM_BINARY} login\" on the VM" >&2
  exit 2
fi
if [ -z "$ENV_VAR" ] || [ -z "$VM_ENVF" ]; then
  echo "provision_remote_auth: registry returned an env_file port without env_var/env_file" >&2
  exit 2
fi

# VM_ENVF is used verbatim in the remote shell (its literal $HOME expands
# there). LOCAL_ENVF is the same path expanded here for the --host-less run
# and the key-reuse fallback.
# shellcheck disable=SC2016
_home_tok='$HOME/'
case "$VM_ENVF" in
  "$_home_tok"*) LOCAL_ENVF="${HOME}/${VM_ENVF#"$_home_tok"}" ;;
  /*) LOCAL_ENVF="$VM_ENVF" ;;
  *)
    echo "provision_remote_auth: registry rendered an unexpected env_file path '${VM_ENVF}'" >&2
    exit 2
    ;;
esac
KEY_VALUE=""

_read_key() {
  if [ -n "${!ENV_VAR:-}" ]; then
    if [ -n "$KEY_FILE" ]; then
      echo "provision_remote_auth: both ${ENV_VAR} (env) and --key-file present; using env" >&2
    fi
    KEY_VALUE="${!ENV_VAR}"
    return 0
  fi
  if [ -n "$KEY_FILE" ]; then
    KEY_VALUE="$(cat "$KEY_FILE")"
    return 0
  fi
  # Reuse the key this script already wrote locally rather than re-prompting.
  # LOCAL_ENVF is the same file the local write path uses, so a second run
  # (or a re-provision after the remote env file is lost) is non-interactive.
  if [ -f "$LOCAL_ENVF" ]; then
    # shellcheck disable=SC1090
    . "$LOCAL_ENVF"
    if [ -n "${!ENV_VAR:-}" ]; then
      KEY_VALUE="${!ENV_VAR}"
      echo "provision_remote_auth: read key from ${LOCAL_ENVF}" >&2
      return 0
    fi
  fi
  if [ ! -t 0 ]; then
    echo "provision_remote_auth: ${ENV_VAR} unset and stdin is not a TTY" >&2
    exit 2
  fi
  # shellcheck disable=SC2162
  read -s -p "${ENV_VAR}: " KEY_VALUE
  echo >&2
}

_validate_key() {
  case "${KEY_VALUE}" in
    ""|*[[:space:]]*)
      echo "provision_remote_auth: empty or whitespace key refused" >&2
      exit 2
      ;;
  esac
  case "${KEY_VALUE}" in
    *$'\n'*|*$'\r'*)
      echo "provision_remote_auth: newline in key refused" >&2
      exit 2
      ;;
  esac
  case "${KEY_VALUE}" in
    \'*|\"*|*\'|*\")
      echo "provision_remote_auth: quoted key refused (pass the bare value)" >&2
      exit 2
      ;;
  esac
  # Keep the value shell-local only: drop the export so child processes (ssh,
  # the remote probe, etc.) do not inherit the key via their environment.
  # The shell variable itself remains set for stdin-streaming and %q writes.
  # bash-3.2-safe [WEB-16 / implementation note S6-H03].
  # shellcheck disable=SC2163
  export -n "$ENV_VAR"
}

_write_env_file() {
  local env_path="$1"
  mkdir -p "$(dirname "$env_path")"
  umask 077
  # bash %q -> shell-safe single-token value for `set -a; . file` [WEB-16]
  printf '%s=%q\n' "$ENV_VAR" "$KEY_VALUE" >"$env_path"
  chmod 600 "$env_path"
  local mode
  mode="$(stat -c %a "$env_path" 2>/dev/null || stat -f %Lp "$env_path")"
  echo "provision_remote_auth: wrote ${env_path} (mode ${mode})" >&2
}

# Login-shell loader for the env file (idempotent; grep -F on the rendered
# env path). Streamed over stdin (ssh 'bash -s' or local bash -s); carries no
# secret. Runs on --env-already-written too.
_profile_loader_script() {
  cat <<EOF
profile="\$HOME/.profile"
if [ -f "\$profile" ] && grep -qF '${VM_ENVF}' "\$profile" 2>/dev/null; then
  exit 0
fi
cat >>"\$profile" <<'LOADER'

# ${BACKEND} credential port — docs/runbooks/remote-gate-provisioning.md
if [ -f "${VM_ENVF}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${VM_ENVF}"
  set +a
fi
LOADER
echo "provision_remote_auth: appended loader to \$profile" >&2
EOF
}

# Run a stdin-streamed bash script either on the VM (--host) or locally.
# env -u is defense in depth: neither path needs the key and must not carry
# it even if a future edit re-exports.
_run_streamed() {
  if [ -n "$HOST" ]; then
    env -u "$ENV_VAR" ssh -o BatchMode=yes -- "$HOST" 'bash -s'
  else
    env -u "$ENV_VAR" bash -s
  fi
}

_smoke() {
  local where="locally"
  [ -n "$HOST" ] && where="on ${HOST}"
  echo "provision_remote_auth: probing ${BACKEND} ${where} (registry probe, status: ${VM_BINARY} ${PORT_STATUS_ARGV})" >&2
  local rc=0
  printf '%s\n' "$PORT_PROBE" | _run_streamed || rc=$?
  case "$rc" in
    0)
      echo "provision_remote_auth: probe ok" >&2
      ;;
    10|11|12|13|14|15)
      echo "provision_remote_auth: probe exited ${rc}" >&2
      exit "$rc"
      ;;
    255)
      if [ -n "$HOST" ]; then
        echo "provision_remote_auth: ssh transport failure (rc 255), not a probe verdict" >&2
      else
        echo "provision_remote_auth: unexpected probe rc 255" >&2
      fi
      exit 255
      ;;
    *)
      echo "provision_remote_auth: unexpected probe rc ${rc}" >&2
      exit "$rc"
      ;;
  esac
}

_finalize() {
  local rc=0
  _profile_loader_script | _run_streamed || rc=$?
  if [ "$rc" -ne 0 ]; then
    if [ -n "$HOST" ]; then
      echo "provision_remote_auth: loader stream failed (ssh rc ${rc})" >&2
    else
      echo "provision_remote_auth: loader stream failed (bash rc ${rc})" >&2
    fi
    exit "$rc"
  fi
  if [ "$SKIP_SMOKE" -eq 0 ]; then
    _smoke
  else
    echo "provision_remote_auth: smoke skipped (--skip-smoke)" >&2
  fi
}

if [ "$ENV_ALREADY_WRITTEN" -eq 1 ]; then
  [ -n "$HOST" ] || test -f "$LOCAL_ENVF"
  _finalize
  exit 0
fi

_read_key
_validate_key
if [ -n "$HOST" ]; then
  # Stream the env file body over ssh stdin — key off argv and out of the
  # child environment (export -n above) [WEB-16 / implementation note S6-H03].
  printf '%s=%q\n' "$ENV_VAR" "$KEY_VALUE" | env -u "$ENV_VAR" ssh -o BatchMode=yes -- "$HOST" \
    "umask 077; mkdir -p \"\$(dirname \"${VM_ENVF}\")\"; cat > \"${VM_ENVF}\"; chmod 600 \"${VM_ENVF}\"; echo \"wrote ${VM_ENVF}\" >&2"
else
  _write_env_file "$LOCAL_ENVF"
fi
_finalize
