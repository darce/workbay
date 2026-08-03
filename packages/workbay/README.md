# workbay

> **Start here — install entry point.** Register the public marketplace, then install the WorkBay stack from GitHub (no PyPI).

In Claude Code, register the public plugin marketplace once per checkout:

```text
/plugin marketplace add darce/workbay
```

Install the git-sourced front door and hoist the overlay into any repository:

```sh
REF=v0.1.35
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
workbay install --target <repo>
# optional: also materialize the codebase-graph MCP (requires codebase-memory-mcp on PATH)
workbay install --target <repo> --with-codebase-graph
```

or install the bootstrap CLI directly and pin a consumer tag:

```sh
REF=v0.1.35
R="git+https://github.com/darce/workbay.git@$REF"
uv tool install --no-sources \
  --with "$R#subdirectory=packages/workbay-protocol" \
  --with "$R#subdirectory=packages/workbay-system" \
  --from "$R#subdirectory=packages/workbay-bootstrap" \
  workbay-bootstrap
workbay-bootstrap install --target <repo> --remote-ref "$REF"
```

`workbay install` delegates to `workbay-bootstrap`. The installer clones
`darce/workbay` at the pinned ref (or uses the in-tree payload when you
installed from git), materializes overlay surfaces, registers both MCP servers
through `scripts/hooks/mcp_launch.py`, and provisions `.task-state/`.

See [`docs/CONSUMER.md`](https://github.com/darce/workbay/blob/main/docs/CONSUMER.md)
for the full install guide.
