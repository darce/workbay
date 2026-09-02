# Contract Change Checklist

> Checklist for cross-boundary changes. Use when a change touches a payload, status code, envelope, enum, or boundary behavior shared across services, languages, or MCP surfaces.

## Boundary Ownership Registry

| Boundary | Canonical Owner | Contract | Adaptation Point | Primary Consumers |
| --- | --- | --- | --- | --- |
| MCP handoff surface | `agentic-tooling` | `docs/workbay/contracts/workbay-handoff-mcp.md` | Installed `workbay-handoff-mcp` API and CLI surface | Orchestrator, workers, review flows |

## Contract-Change Steps

1. Load the owning contract before editing code.
2. Confirm the canonical owner. If multiple layers adapt the shape, collapse ownership first.
3. Classify the change: payload shape, status/error semantics, enum vocabulary, pagination/provenance metadata, or runtime parity.
4. Update the owning contract in the same slice. If unchanged, record a handoff decision explaining why.
   For test-only cleanup on boundary-touching files, add a same-slice checklist or contract note that explicitly states the runtime contract is unchanged.
5. Update shared schema/fixture in the same slice.
6. Add deterministic proof: fixture/schema assertion, contract tests, runtime-parity proof.
7. Record a handoff decision: boundary, owning contract, verification path, compatibility stance, valid downstream assumptions.
8. Slice is not review-ready until contract, schema/fixture, tests, and handoff proof all exist together.

## Contract Intake Template

Use this template when opening or implementing a cross-boundary slice:

```md
Boundary changed:
Owning contract:
Canonical owner:
Compatibility required: yes/no
Changed fields/status semantics:
Shared schema or fixture touched:
Verification proof:
Downstream adapters allowed to assume:
Handoff decision id:
```

## Schema-Evolution Notes

Attach to the owning contract or implementation decision when a shared payload changes:

```md
Schema evolution:
- What changed:
- Why it changed:
- Compatibility required: yes/no
- Canonical owner:
- Downstream consumers affected:
- Fixture/schema/test updated:
```

Rules:

- Greenfield default: `Compatibility required: no`.
- No backward-compatibility shims unless explicitly documented.
- Downstream consumers validate the canonical shape only.

## Healthy Data Patterns

- One writer per fact.
- Explicit provenance for derived metadata.
- No silent dual-write drift: document which layer is canonical.
- Name read-after-write expectations: immediate, eventual, or best-effort.
- Adapters must not invent pagination, provenance, or status metadata.
- Unavailability and malformed payloads stay distinct from true empty results.

## Canonical Enum and Constant Surfaces

| Concept | Canonical Surface |
| --- | --- |
| MCP review modes and status values | `docs/workbay/contracts/workbay-handoff-mcp.md` and installed `workbay-handoff-mcp` |

## Remediation-Plan Finding IDs

Cited `finding_id` values must resolve to a real MCP finding or concrete code site before implementation. Fix/archive/defer existing findings through MCP. Record a decision for non-existent IDs. Do not carry unverifiable IDs as assumed debt.
