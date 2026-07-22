# Semantic-capability gating (`embeddings_mode`)

Operational rules for skills and tools that consume semantic embeddings
(reinjection packets, recall guidance, semantic search suggestions). This is
the durable home for install-time embeddings capability policy; skills link
here instead of restating it. Ledger values are authoritative via the named
reader — not this page.

## When semantic features may be relied on

- The gate is **install-scoped, not ambient**: hard reliance on semantic
  features applies iff the bootstrap ledger records
  `embeddings_mode: verified` (written by `workbay-bootstrap install
  --with-embeddings`, hard-verified at install). Read only via
  `workbay_protocol.bootstrap.load_embeddings_mode`.
- **`verified`**: semantic features may be relied on. If the runtime still
  cannot honor embeddings, the handoff surface flags the degrade (below) —
  skills **surface** it, never silently treat lexical-only output as full
  semantic success.
- **`unspecified`** (default, including a missing ledger or field):
  best-effort only. Provisioning and selection keep pre-flag behavior; the
  typed degrade flag stays **absent by design** (null) so consumers do not
  treat an unflagged install as a broken verified one.
- **`disabled`** (`install --no-embeddings`): never attempt embeddings
  provisioning or hard reliance. Semantic paths stay off; do not probe or
  provision to "try anyway."

## Response fields consumers must read

Handoff-server semantic surfaces — notably
`semantic_reinjection_packet` — echo:

| Field | Role |
| --- | --- |
| `embeddings_mode` | Ledger echo (`unspecified` \| `verified` \| `disabled`), sourced through `load_embeddings_mode` |
| `semantic_degrade_reason` | Non-null only when the operator installed `--with-embeddings` (`verified`) but the runtime fell back from semantic selection — the native "flag when not available" signal |

- A **non-null** `semantic_degrade_reason` means: embeddings were promised at
  install, but this call could not honor them. Surface that to the operator
  (decision/blocker-style note as appropriate). Do **not** silently accept
  lexical-only content as if semantic reinjection succeeded.
- Under `unspecified` / `disabled`, `semantic_degrade_reason` remains null by
  design; existing `status` / `skip_reason` semantics are unchanged.
- Field relationship (single source, never populated independently):
  `skip_reason` keeps its existing meaning (why semantic selection was
  skipped entirely); `semantic_degrade_reason` states why a verified install
  fell back while still returning content. Where conditions coincide, the
  server derives one from the other — skills read both, invent neither.
- `search_handoff` is lexical FTS today and is out of scope for this gate
  until a semantic search mode exists.

## Skill posture

- **Flag, never assume**: absence of embeddings under a `verified` install is
  a visible degrade, not an empty success.
- Dropping the capability is an operator act (`repair` / install flag
  changes), not a silent skill downgrade.
- Runtime embeddings on/off toggles (`workbay embeddings`) are a separate
  control surface; they do not rewrite the install-time ledger. Still honor
  `embeddings_mode` + `semantic_degrade_reason` on packet responses.
