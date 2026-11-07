# workbay-codex-bridge

> **Component of the `workbay` stack.** You usually install `workbay`, not this package directly.

Optional bridge module that satisfies the `workbay_codex_bridge.run_subagent(...)` contract used by the orchestration layer.

It launches `codex app-server --listen stdio://`, drives one structured turn, and returns the final payload as a Python `dict`.

## Installation

Use Python 3.12 or later and a validated full monorepo checkout with its
`uv.lock`. Run from the workspace root:

```bash
cd /path/to/workbay
uv sync --locked --all-packages --extra dev
source .venv/bin/activate
```

This keeps all eight WorkBay members editable from the same workspace via
`tool.uv.sources` and the lockfile. A leaf `pip install -e` does not honor those
source mappings and leaves the protocol dependency unbound. Third-party
dependencies may still use the package index or cache.

If you do not want to install it, add `src` to `PYTHONPATH` or inject `workbay_codex_bridge` into `sys.modules` from the host process.

## Development

Run package-local commands from the package root:

```bash
make lint-bridge
make fix-lint-bridge
make format-bridge
make mypy-bridge
make test-bridge
make check-bridge
```

Direct commands also work:

```bash
PYTHONPATH=src python -m ruff check src tests
PYTHONPATH=src python -m mypy src
PYTHONPATH=src python -m pytest tests -q
```

## Runtime Behavior

- The bridge keeps the existing seam: `prompt + schema + cwd + optional env -> dict`
- `env` values are treated as local runtime hints only and become subprocess or session context for `codex app-server`
- `CODEX_REASONING_EFFORT` or `REASONING_EFFORT` map to `turn/start.effort` when set to `low`, `medium`, `high`, or `xhigh`
- MCP endpoints and credentials are not forwarded through the bridge
- Build and test commands must still come from the worktree instruction surface or the rendered prompt
- `run_subagent()` is concurrency-safe because each default call launches and tears down its own app-server process
- `CODEX_SUBAGENT_BRIDGE_SESSION_MODE=shared` opts into a reusable app-server process for compatible calls in the same `cwd`

## Integration Note

This package is intentionally small. It provides the bridge seam only; higher-level orchestration policy belongs in the caller.
