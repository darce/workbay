# workbay

> **Start here — install entry point.** Register the public marketplace, then install the WorkBay stack from GitHub (no PyPI).

In Claude Code, register the public plugin marketplace once per checkout:

```text
/plugin marketplace add darce/workbay
```

Install the git-sourced front door and hoist the overlay into any repository:

```sh
uv tool install "git+https://github.com/darce/workbay@v0.2.1#subdirectory=packages/workbay"
workbay install --target <repo>
```

or install the bootstrap CLI directly and pin a consumer tag:

```sh
uv tool install "git+https://github.com/darce/workbay@v0.2.1#subdirectory=packages/workbay-bootstrap"
workbay-bootstrap install --target <repo> --remote-ref v0.2.1
```

`workbay install` delegates to `workbay-bootstrap`. The installer clones
`darce/workbay` at the pinned ref (or uses the in-tree payload when you
installed from git), materializes overlay surfaces, registers both MCP servers
through `scripts/hooks/mcp_launch.py`, and provisions `.task-state/`.

See [`docs/CONSUMER.md`](https://github.com/darce/workbay/blob/main/docs/CONSUMER.md)
for the full install guide.
