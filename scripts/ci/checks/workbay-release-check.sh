#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."
python_cli=python3
if [[ -x .venv/bin/python ]]; then
  python_cli=.venv/bin/python
fi

"$python_cli" scripts/check_release_version_drift.py
exec "$python_cli" scripts/release_public.py --dry-run
