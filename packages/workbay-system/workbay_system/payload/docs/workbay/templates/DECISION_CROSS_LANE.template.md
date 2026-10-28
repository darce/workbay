# Cross-Lane Dependency Decision Template

> **Metadata** — fill in when creating a new doc from this template:
>
> - **Date**: [YYYY-MM-DD HH:MM EST]
> - **Author**: {{MODEL_IDENTITY}}
>
> Use this template when one lane creates a dependency, blocker, or required follow-up for another lane.

Reference: [../rules/development-workflow.md#cross-boundary-change-protocol](../rules/development-workflow.md#cross-boundary-change-protocol)

## Fields

Required:

- `source_lane`: Lane creating the dependency.
- `target_lane`: Lane that must react.
- `dependency_type`: Contract, schema, type, test fixture, runtime behavior, or similar.
- `required_follow_up`: What the target lane needs to do.
- `evidence`: Contract diff, test output, type failure, or runtime proof justifying the dependency.
- `urgency`: `blocking` or `informational`.

Optional:

- `contract_path`: Owning contract document path, if applicable.
- `verification_command`: Command that demonstrates the dependency or breakage.
- `assumption_window`: What is safe until the target lane completes the follow-up.

## Suggested Decision Body

```text
source_lane:
target_lane:
dependency_type:
contract_path:
required_follow_up:
evidence:
verification_command:
urgency:
assumption_window:
```
