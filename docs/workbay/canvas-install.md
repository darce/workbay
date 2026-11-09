# Install Canvas, Studio, Lab and Fleet

Use Python 3.12 or later and uv. From the public distribution root, create a
fresh environment and install every shipped member together. The distribution
already includes the built browser app, Python backend and inventory collector.

```bash
uv venv .venv
python3 - <<'PY'
from pathlib import Path
import subprocess
import sys
sys.path.insert(0, str(Path('scripts').resolve()))
from check_consumer_install import discover_member_paths, build_pip_install_argv
members = discover_member_paths(Path.cwd())
subprocess.run(build_pip_install_argv(members=members, python=Path('.venv/bin/python').resolve()), check=True)
PY
.venv/bin/workbay studio --help
.venv/bin/workbay studio --workspace-root /path/to/project --port 0 --no-open
.venv/bin/workbay fleet --serve --root /path/to/project --port 0 --no-open
```

Open the printed loopback URL. Studio includes the Lab view; Fleet opens at
`#/fleet`. Keep using this environment's executable when switching projects.
The full local-path recipe keeps all sibling versions from this distribution;
it requires neither a workspace root nor a source override suppression flag.
Third-party Python dependencies need access to the configured index or cache.

Viewing needs no frontend build or Node installation. Inventory Refresh needs
Node.js >=20 and TypeScript >=5 plus PostCSS >=8 in the consumer workspace
(`npm install -D typescript postcss`). `WORKBAY_NODE` selects Node;
`WORKBAY_INVENTORY_COLLECTOR` optionally selects a collector script. PNG export
additionally requires CairoSVG and native libcairo in the same environment.
