#!/usr/bin/env bash
set -euo pipefail
umask 077
exec </dev/null

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python="${script_dir}/../../.venv/bin/python"
if [[ ! -x "$python" ]]; then
    python=python3
fi
exec "$python" "${script_dir}/vm_ci_workbay_publish.py" "$@"
