# workbay

> **Start here — install entry point.** Register the public marketplace, or
> install the CLI from a tagged GitHub ref (GitHub-only delivery).

In Claude Code, register the public plugin marketplace once per checkout:

```text
/plugin marketplace add darce/workbay
```

Install the front-door CLI (helper expands the multi-package git closure), then
hoist the overlay:

```sh
REF=v0.1.54
curl -fsSL "https://raw.githubusercontent.com/darce/workbay/${REF}/scripts/install-workbay-cli.sh" \
  | bash -s -- "$REF"
workbay install --target <repo> --remote-ref "$REF"
# optional: also materialize the codebase-graph MCP (requires codebase-memory-mcp on PATH)
workbay install --target <repo> --remote-ref "$REF" --with-codebase-graph
```

From a checkout of the same tag: `./scripts/install-workbay-cli.sh "$REF"`.

On locked-down hosts, install the CLI with the explicit
`uv tool install --no-sources` closure so the full runtime package list is
written out:

```sh
REF=v0.1.54
R="git+https://github.com/darce/workbay.git@$REF"
# --no-sources is required (each member pyproject carries a workspace source);
# the whole runtime closure is git-sourced via --with (never PyPI).
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/mcp-workbay-handoff" \
  --with "$R#subdirectory=packages/mcp-workbay-orchestrator" \
  --with "$R#subdirectory=packages/workbay-bootstrap" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay" \
  workbay
workbay install --target /path/to/repo --remote-ref "$REF"
# optional flags (see docs/CONSUMER.md and the root README):
#   --with-codebase-graph  register optional codebase-graph MCP
#   --with-remote          probe remote gate; record remote_only offload policy
#   --with-embeddings      hard-verify embeddings extra + digest pin
#   --no-embeddings        skip embedding model download during install
workbay install --target /path/to/repo --remote-ref "$REF" --with-codebase-graph
```

Or install the bootstrap CLI directly and pin a consumer tag:

```sh
REF=v0.1.54
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay-bootstrap" \
  workbay-bootstrap
workbay-bootstrap install --target /path/to/repo --remote-ref "$REF"
```

`workbay install` delegates to `workbay-bootstrap`. The installer clones
`darce/workbay` at the pinned ref (or uses the in-tree payload when you
installed from git), materializes overlay surfaces, registers both MCP servers
through `scripts/hooks/mcp_launch.py`, and provisions `.task-state/`.

See [`docs/CONSUMER.md`](https://github.com/darce/workbay/blob/main/docs/CONSUMER.md)
for the full install guide, including the manual `uv` escape hatch.
