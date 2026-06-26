# workbay

> **Start here — install entry point.** This is the single command that installs the WorkBay stack.

The WorkBay front door. One command hoists the WorkBay agentic-workflow
surface into any repository:

```sh
uvx workbay install --target <repo>
```

or install the entry point and run it directly:

```sh
uv tool install workbay
workbay install --target <repo>
```

`workbay install` delegates to `workbay-bootstrap`, defaulting the overlay
source to the published distribution payload (`--source package`). Pass any
other `workbay-bootstrap` subcommand — `doctor`, `status`, `update` — straight
through.

This distribution carries no runtime code of its own: it pins the published
`workbay-stack` runtime (`workbay-stack==0.1.0`) and the bootstrap installer it
delegates to. It is a *consumer* of `workbay-stack`, not a stack member. See
[`docs/CONSUMER.md`](https://github.com/darce/workbay/blob/main/docs/CONSUMER.md)
for the full install guide.
