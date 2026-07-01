# WorkBay Orchestrator MCP

> **Component of the `workbay` stack.** You usually install `workbay`, not this package directly.

MCP server for orchestration, lane management, worker daemons, review dispatch, and ACE metrics.

## Installation

### From PyPI (recommended)

```bash
pip install mcp-workbay-orchestrator
# or, as an isolated tool:
uv tool install mcp-workbay-orchestrator
# or, ad-hoc without installing:
uvx mcp-workbay-orchestrator --help
```

`mcp-workbay-orchestrator` declares `mcp-workbay-handoff>=0.5.0,<0.6.0` as a
required dependency; pip resolves it from PyPI automatically.
`workbay-codex-bridge` remains optional unless you want the local
bridge backend.

### From the monorepo source tree (development)

From this package root inside `workbay`:

```bash
cd packages/mcp-workbay-orchestrator
python -m pip install -e ".[dev]"
```

When developing both MCP servers in lockstep, install the sibling
handoff package as an editable first so the orchestrator picks it up:

```bash
pip install -e ../mcp-workbay-handoff
pip install -e ".[dev]"
```

## Development

Run package-local commands from the package root:

```bash
make lint-orchestrator
make fix-lint-orchestrator
make format-orchestrator
make mypy-orchestrator
make test-orchestrator
make check-orchestrator
```

The package Makefile keeps `workbay-codex-bridge` as an optional sibling source path for local bridge-backend development, but it expects `mcp-workbay-handoff` to be installed as a normal package dependency.

Direct commands also work:

```bash
PYTHONPATH=src python -m ruff check src tests
PYTHONPATH=src python -m mypy src
PYTHONPATH=src python -m pytest tests -q
```

## Token-Efficient Usage

For bounded reads and compact caller patterns, follow the shared guide in [`packages/mcp-workbay-handoff/docs/guides/token-efficient-usage.md`](../mcp-workbay-handoff/docs/guides/token-efficient-usage.md). The orchestrator package reuses that guidance instead of maintaining a separate copy of the same parameter semantics.

## Runtime Notes

This package orchestrates work against a target workspace. The workspace you point it at still needs the expected task state and orchestration inputs, such as:

- `.task-state/`
- lane manifests
- task plans or other orchestration docs the lane logic references

Those assets belong to the workspace being orchestrated, not to the package checkout itself.

## Backends

The orchestration layer supports multiple execution backends, including:

- `codex-cli`
- `codex-subagent`
- `claude-code`
- `local-model-openai`

Some backends are optional and require host-specific tooling to be installed separately.

### Availability vs. the optional bridge

The static backend table always lists every declared backend. That a backend is
*listed* does not mean it will *run* in the current process. The `codex-subagent`
backend needs the optional `workbay-codex-bridge` host module, which is **not** a
base dependency and is **not** installed by the bootstrap presync (the launcher
runs `uv run --no-sync`). If the orchestrator server launches from a venv that
lacks the bridge, `resolve_bridge("codex-subagent")` raises `ImportError` at
dispatch even though the backend is listed.

To surface this without a live turn, call the MCP tool `list_available_backends`
with its default settings, or call the CLI with probing on:

```bash
mcp-workbay-orchestrator list-backends --probe
```

Each probed backend carries an `availability_state`:

- `available` — in-process adapter, or a CLI binary found on PATH.
- `reachable` — a bridge module imports and exposes a runner; **liveness is not
  verified** (a real turn may still time out at dispatch).
- `declared_not_installed` — the backend is declared but its optional host module
  (e.g. `workbay-codex-bridge`) is not importable in this runtime.
- `unavailable` — a CLI backend whose binary is not on PATH, or a bridge module
  that imports but does not expose the required runner.
- `unknown` — no probe is implemented for that backend kind.

Install the bridge on demand to move `codex-subagent` from
`declared_not_installed` to `reachable`:

```bash
uv sync --extra bridge   # resolves workbay-codex-bridge from the sibling source
```

## Source Checkout Usage

For local source execution without installation:

```bash
PYTHONPATH=src python -m workbay_orchestrator_mcp --help
```

If you are testing against a sibling `workbay-codex-bridge` checkout instead of an installed bridge dependency, extend `PYTHONPATH` with that sibling `src` directory as needed.

## Lane-data CLI (bash workflows)

`mcp-workbay-handoff` 0.12.0 dropped lane-data subcommands. Bash and
`scripts/worktree-lane` call the orchestrator CLI instead:

| Subcommand | `lanes.py` adapter |
| --- | --- |
| `lane-upsert` | `manage_worktree_lane(operation="upsert")` |
| `lane-list` | `manage_worktree_lane(operation="list")` |
| `lane-activity` | `get_lane_activity` |
| `lane-message` | `lane_communication(kind="message", operation="record")` |
| `lane-message-list` | `lane_communication(kind="message", operation="list")` |
| `lane-message-update` | `lane_communication(kind="message", operation="update")` |
| `lane-report` | `worker_reports(operation="record")` |
| `lane-report-list` | `worker_reports(operation="list")` |

All lane subcommands print JSON to stdout and exit `1` when the payload has
`ok: false`. Handoff reads (`state`, `integrity-check --kind close`, etc.)
remain on `mcp-workbay-handoff`.

Example:

```bash
mcp-workbay-orchestrator --workspace-root "$ROOT" lane-list --task-ref TASK-1 --status all
```
