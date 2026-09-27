---
template_for: record_decision slice_complete_* rationale
---

# Slice Completion Decision Format

Use this format as the `rationale` value for every canonical slice-complete
decision. Check the authoritative registry at
`get_handoff_state(sections="identity").data.limits.write.slice_complete_decision_id`
or preflight with `validate(payload={"kind": "decision_id", "decision": ..., "decision_kind": "slice_complete"})`
before writing. Valid: `cdx_slice_complete_plan0004_contract_pinning_and_docs`.
Invalid: `cdx_slice_complete_plan0004_contract-pinning-and-docs`.

Pass `slice_number` (integer from `### Checklist for Slice N` in the task plan)
on `close_slice` so plan-checklist sync binds evidence to the correct slice.
The slug does not need to embed `slice_N`; omitting `slice_number` still works
via a legacy slug-regex fallback but emits a receipt warning.

```
## Changes
- <file_path>: <function_or_class_name> ; <what changed>
- <file_path>: <route_or_endpoint> ; <what changed>

## Verification
- pytest <path>: <N> passed
- vitest <path>: <N> passed
- mypy: <N> source files clean

## Schema / Contract Changes
- <table.column> added/removed/renamed
- <REST route> added/removed ; <method> <path>
- <TypeScript type> field added: <field_name>: <type>

## Open Threads
- <what the next agent should pick up>
```

## Rules

- (a) List every changed file with the specific function, class, route, or hook modified.
- (b) Include concrete test counts, not just "tests pass".
- (c) List schema column names, REST routes, TypeScript type changes, and PHP hook names explicitly.
- (d) Note any open threads or follow-ups.
- (e) If a section has no entries, write `- none.` -- do not omit the section.
- (f) Freeform prose-only slice decisions are not acceptable.

## Packet-backed review rule

If you expect another agent to review "the latest completed slice", the slice completion
decision and nearest worker report together must be sufficient to derive a slice review
packet. Record concrete `changed_files`, verification commands, and any contract/doc
touches in the same handoff window instead of relying on later branch archaeology.
