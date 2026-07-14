# Subagent Bridge Interface Note

The orchestration layer depends on a very small execution seam:

```python
def run_subagent(
    prompt: str,
    schema: dict,
    cwd: str,
    env: dict | None = None,
) -> dict | str: ...
```

Bridge backends are now registered centrally through
[`backend_registry.py`](../../../packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/backend_registry.py).
`lane_exec.py` and `review_runner.py` no longer hardcode bridge imports directly;
they validate the backend name through the registry and dispatch any backend whose
registered `kind` is `"bridge"` through `resolve_bridge(backend_name)`.

That means adding a new bridge backend should not require edits to the lane/review
execution callers. The expected shape is:

```python
from backend_registry import BACKENDS, BackendSpec

BACKENDS["kimi-host"] = BackendSpec(
    kind="bridge",
    module="kimi_host_bridge",
    description="Kimi host bridge via structured prompt API.",
)
```

The referenced module must expose:

```python
def run_subagent(
    prompt: str,
    schema: dict,
    cwd: str,
    env: dict | None = None,
) -> dict | str: ...
```

That seam is intentionally host-oriented rather than Codex-specific. A non-Codex
adapter can fit without changing `lane_exec.py` or `review_runner.py` as long as it:

- accepts the fully rendered prompt
- constrains or validates final output against the supplied JSON schema
- executes in the requested worktree `cwd`
- treats `env` as optional local runtime hints, not as permission to mutate MCP state
- returns either a Python `dict` or a JSON string that parses into one

Minimal non-Codex example:

```python
def run_subagent(prompt: str, schema: dict, cwd: str, env: dict | None = None) -> dict:
    client = KimiHostClient(cwd=cwd, env=env)
    try:
        client.connect()
        return client.run_structured_prompt(prompt=prompt, output_schema=schema)
    finally:
        client.close()
```

What should stay outside any bridge implementation:

- MCP handoff writes
- review finding persistence
- lane routing or manifest logic
- result-schema ownership
- daemon lifecycle management

Those responsibilities remain in the parent daemon process so bridges stay portable
and easy to swap.
