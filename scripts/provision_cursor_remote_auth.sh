#!/usr/bin/env bash
# Shim (implementation note S3): cursor-remote is one row of the registry-derived provisioner.
# Resolves its own location through symlinks (macOS has no readlink -f).
self="${BASH_SOURCE[0]}"
while [ -L "$self" ]; do
  target="$(readlink "$self")"
  case "$target" in
    /*) self="$target" ;;
    *) self="$(dirname "$self")/${target}" ;;
  esac
done
exec "$(cd "$(dirname "$self")" && pwd)/provision_remote_auth.sh" --backend cursor-remote "$@"
