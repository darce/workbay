# Contract Change Decision Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
>
> Use this template when a slice changes a service, schema, REST, type, or MCP contract.

Reference: [../rules/development-workflow.md#cross-boundary-change-protocol](../rules/development-workflow.md#cross-boundary-change-protocol)

## Fields

Required:

- `boundary_name`: Short name for the boundary or adapter surface.
- `contract_path`: Owning contract document path.
- `fields_changed`: Added, removed, renamed, or redefined fields and behaviors.
- `tests_verifying_change`: Verification commands with pass counts or equivalent proof.
- `downstream_consumers_checked`: Consumers reviewed or tested against the new shape.
- `assumptions_safe_to_make`: What later agents can safely assume without rediscovery.

Optional:

- `ctx7_library_id`: Resolved `ctx7` library id if upstream docs influenced the change.
- `runtime_parity_check`: Real-path validation run or the gap that prevented it.
- `follow_up_required`: Explicit remaining work if the slice intentionally left anything open.

## Suggested Decision Body

```text
boundary_name:
contract_path:
fields_changed:
tests_verifying_change:
downstream_consumers_checked:
assumptions_safe_to_make:
ctx7_library_id:
runtime_parity_check:
follow_up_required:
```
