# Breaking Change Decision Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
>
> Use this template when a slice intentionally removes or changes a contract shape in a way that would break an existing consumer without coordinated follow-up.

Reference: [../rules/development-workflow.md#cross-boundary-change-protocol](../rules/development-workflow.md#cross-boundary-change-protocol)

## Fields

Required:

- `boundary_name`: Short name for the breaking surface.
- `what_changed`: The removed or incompatible behavior.
- `why_change_is_required`: Why the old shape is unsafe, wrong, or obsolete.
- `consumer_migration`: What downstream consumers must do next.
- `tests_proving_new_shape`: Verification commands and pass counts.
- `lanes_or_owners_notified`: Lanes, stacks, or owners told about the change.

Optional:

- `contract_path`: Owning contract document, if one exists.
- `deprecation_timeline`: Use `greenfield; no deprecation needed` when applicable.
- `ctx7_library_id`: Resolved `ctx7` library id if upstream docs informed the decision.

## Suggested Decision Body

```text
boundary_name:
contract_path:
what_changed:
why_change_is_required:
consumer_migration:
deprecation_timeline:
tests_proving_new_shape:
lanes_or_owners_notified:
ctx7_library_id:
```
