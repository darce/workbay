#!/usr/bin/env bash
# Provision CURSOR_API_KEY on the OCI gate VM for cursor-remote (implementation note D6).
#
# Writes an out-of-tree env file (never under the git clone):
#   ~/.config/cursor-agent/env   (mode 0600)
#
# Usage (on the VM as gate):
#   CURSOR_API_KEY='…' ./scripts/provision_cursor_remote_auth.sh
#   ./scripts/provision_cursor_remote_auth.sh --key-file /path/to/key
#   ./scripts/provision_cursor_remote_auth.sh   # prompts (no echo)
#
# From the laptop (key never appears on the ssh argv):
#   CURSOR_API_KEY='…' ./scripts/provision_cursor_remote_auth.sh --host gate@<host>
#   ./scripts/provision_cursor_remote_auth.sh --host gate@<host> --key-file ./key
#
# Docs: docs/runbooks/remote-gate-provisioning.md (Cursor section).
set -euo pipefail

HOST=""
KEY_FILE=""
SKIP_SMOKE=0
ENV_ALREADY_WRITTEN=0
ENV_PATH="${HOME}/.config/cursor-agent/env"

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --host) HOST="${2:-}"; shift 2 ;;
    --key-file) KEY_FILE="${2:-}"; shift 2 ;;
    --skip-smoke) SKIP_SMOKE=1; shift ;;
    --env-already-written) ENV_ALREADY_WRITTEN=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

_read_key() {
  if [ -n "${CURSOR_API_KEY:-}" ]; then
    return 0
  fi
  if [ -n "$KEY_FILE" ]; then
    CURSOR_API_KEY="$(cat "$KEY_FILE")"
    return 0
  fi
  # Reuse the key this script already wrote locally rather than re-prompting.
  # ENV_PATH is the same file _write_env_file and _smoke use, so a second run
  # (or a re-provision after the remote env file is lost) is non-interactive.
  if [ -f "$ENV_PATH" ]; then
    # shellcheck disable=SC1090
    . "$ENV_PATH"
    if [ -n "${CURSOR_API_KEY:-}" ]; then
      echo "provision_cursor_remote_auth: read key from ${ENV_PATH}" >&2
      return 0
    fi
  fi
  if [ ! -t 0 ]; then
    echo "provision_cursor_remote_auth: CURSOR_API_KEY unset and stdin is not a TTY" >&2
    exit 2
  fi
  # shellcheck disable=SC2162
  read -s -p "CURSOR_API_KEY: " CURSOR_API_KEY
  echo >&2
}

_validate_key() {
  case "${CURSOR_API_KEY}" in
    ""|*[[:space:]]*)
      echo "provision_cursor_remote_auth: empty or whitespace key refused" >&2
      exit 2
      ;;
  esac
  case "${CURSOR_API_KEY}" in
    *$'\n'*|*$'\r'*)
      echo "provision_cursor_remote_auth: newline in key refused" >&2
      exit 2
      ;;
  esac
  # Keep the value shell-local only: drop the export so child processes (ssh,
  # cursor-agent smoke, etc.) do not inherit the key via their environment.
  # The shell variable itself remains set for stdin-streaming and %q writes.
  # bash-3.2-safe [WEB-16 / implementation note S6-H03].
  export -n CURSOR_API_KEY
}

_write_env_file() {
  local env_path="$1"
  mkdir -p "$(dirname "$env_path")"
  umask 077
  # bash %q → shell-safe single-token value for `set -a; . file` [WEB-16]
  printf 'CURSOR_API_KEY=%q\n' "$CURSOR_API_KEY" >"$env_path"
  chmod 600 "$env_path"
  local mode
  mode="$(stat -c %a "$env_path" 2>/dev/null || stat -f %Lp "$env_path")"
  echo "provision_cursor_remote_auth: wrote ${env_path} (mode ${mode})" >&2
}

_ensure_profile_loader() {
  local profile="${HOME}/.profile"
  if [ -f "$profile" ] && grep -q 'cursor-agent/env' "$profile" 2>/dev/null; then
    return 0
  fi
  cat >>"$profile" <<'EOF'

# Cursor agent (OCI lane) — docs/runbooks/remote-gate-provisioning.md
if [ -f "$HOME/.config/cursor-agent/env" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$HOME/.config/cursor-agent/env"
  set +a
fi
EOF
  echo "provision_cursor_remote_auth: appended loader to ${profile}" >&2
}

_resolve_cursor_bin() {
  local bin="${HOME}/.local/bin/cursor-agent"
  if [ -x "$bin" ]; then
    printf '%s\n' "$bin"
    return 0
  fi
  echo "provision_cursor_remote_auth: cursor-agent missing at ${bin} (install per runbook)" >&2
  exit 1
}

_smoke() {
  set -a
  # shellcheck disable=SC1090
  . "$ENV_PATH"
  set +a
  local bin
  bin="$(_resolve_cursor_bin)"
  echo "provision_cursor_remote_auth: smoke via ${bin}" >&2
  local out
  out="$("$bin" -p --force --trust --workspace /tmp --output-format json "ping")"
  printf '%s\n' "$out"
  if ! printf '%s\n' "$out" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d.get("result")=="pong" and not d.get("is_error")' 2>/dev/null; then
    echo "provision_cursor_remote_auth: smoke did not return result=pong" >&2
    exit 1
  fi
  echo "provision_cursor_remote_auth: smoke ok" >&2
}

_finalize_on_vm() {
  _ensure_profile_loader
  if [ "$SKIP_SMOKE" -eq 0 ]; then
    _smoke
  fi
}

# Remote finalize after laptop wrote the env file (single code path).
if [ "$ENV_ALREADY_WRITTEN" -eq 1 ]; then
  test -f "$ENV_PATH"
  _finalize_on_vm
  exit 0
fi

if [ -n "$HOST" ]; then
  _read_key
  _validate_key
  # Stream env file body over ssh stdin — key off argv and out of the child
  # environment (export -n above) [WEB-16 / implementation note S6-H03].
  printf 'CURSOR_API_KEY=%q\n' "$CURSOR_API_KEY" | ssh -o BatchMode=yes "$HOST" \
    'umask 077; mkdir -p "$HOME/.config/cursor-agent"; cat > "$HOME/.config/cursor-agent/env"; chmod 600 "$HOME/.config/cursor-agent/env"; echo "wrote $HOME/.config/cursor-agent/env" >&2'
  self="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
  remote_args=(--env-already-written)
  [ "$SKIP_SMOKE" -eq 1 ] && remote_args+=(--skip-smoke)
  # Re-exec this script on the VM so profile/smoke stay single-sourced.
  # env -u is defense in depth: the re-exec does not need the key (env file
  # already written) and must not carry it even if a future edit re-exports.
  cat "$self" | env -u CURSOR_API_KEY ssh -o BatchMode=yes "$HOST" 'bash -s' -- "${remote_args[@]}"
  exit 0
fi

# --- on-VM / local path -----------------------------------------------------
_read_key
_validate_key
_write_env_file "$ENV_PATH"
_finalize_on_vm
