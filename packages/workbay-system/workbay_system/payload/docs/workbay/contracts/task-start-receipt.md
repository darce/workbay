# `task-start` receipt contract (spool drain)

Additive JSON keys on the success-path `task-start` receipt. Early
`ok:false` exits never reach this block and omit `spool_drain`.

Fixture exemplars:

- Healthy counters + breaker-open skip (healthy-cell only):
  `packages/workbay-system/tests/lifecycle/fixtures/receipts/task_start_spool_drain.json`.
- Unreadable counters cell (both type-strict flags true; no null-only never-read):
  `packages/workbay-system/tests/lifecycle/fixtures/receipts/task_start_spool_drain_unreadable.json`.

## `spool_drain` object

| Key | Type | Meaning |
| --- | --- | --- |
| `attempted` | bool | Foreground `drain_spool` ran (pid-aware orphan gate fired). |
| `ran` | bool \| null | At least one entry was reclaimed (`drained > 0`). Pure skips stay false. `null` when the drained conversion never finished **or** completed as unreadable (tri-state; never a fabricated `false` on the completed-unreadable path). |
| `drained` | int \| null | Reclaimed entry count. When a type-strict parse completed and the value is unparseable/null/absent/negative/non-int, the numeric field is **`null`** and `counters_unparseable` / `drained_unparseable` are true (never a silent honest zero — a fabricated `0` is byte-identical to genuine zero). `null` also when the drained conversion never finished. |
| `pending_remaining` | int \| null | Entries still parked after the attempt (including self-pid writebacks). `null` when never attempted, when the key is absent/null, when the counter is unreadable (null/corrupt/negative/non-int ≠ empty `0`), or when the remaining conversion never finished. When the count finished readable and `pending_remaining_capped` is `null`, an exact integer means **count read, floor unknown** (the floor bit never finished or completed unreadable; do not treat the pair as honest exact-floor). |
| `pending_remaining_capped` | bool \| null | Floor bit for `pending_remaining`. Type-strict real bool only. **Human mapping is preconditioned on remaining being readable** (`pending_remaining_unparseable` is false and the count finished): `true` → human `pending_remaining=N+`; `false` → exact `pending_remaining=N`. When remaining itself is unreadable, human emits `pending_remaining=unknown` and does **not** consult this bit for N/N+ (the bit may still be published so a get-raise can surface as `drain_failed=`). `null` when the floor bit never finished reading **or** completed as unreadable (non-bool value) — must not be reported as a confident `false`. Machine-readable twin of the human floor grammar so a floor of 5 and an exact 5 are not byte-identical on the receipt (SPOOL3-H-01). |
| `pending_remaining_capped_unparseable` | bool \| null | Tri-state twin of `drained_unparseable` for the floor bit. **`true`** when the floor get completed but the value is not a real bool. **`false`** when a real bool was read. **`null`** when the floor get never finished. |
| `counters_unparseable` | bool \| null | Three-valued monotone aggregate over the two counter parses. **`true`** as soon as either of `drained_unparseable` / `pending_remaining_unparseable` **is** `true` (a determined fact; do not wait for the other counter — `TRUE OR UNKNOWN = TRUE`). **`false`** only when **both** type-strict parses completed and both per-counter flags are `false` (healthy readable pair). **`null`** otherwise (partial finish with no determined `true` — never a confident `false` from an unfinished second counter). **When `true`**, opens the post-drain recompute gate **unless** `skipped_reason` is a deliberate projection-path suppression (`projection_breaker_open`, `projection_replay_locked`). **`null` does not open** that unparseable arm (`bool(None)` is false); mid-raise paths may still recompute via non-null `failed` / `drain_error`, which is a separate condition. |
| `drained_unparseable` | bool \| null | Per-counter attribution: `drained` failed the type-strict rule. `null` when that counter's parse never completed. |
| `pending_remaining_unparseable` | bool \| null | Per-counter attribution: `pending_remaining` failed the type-strict rule. `null` when that counter's parse never completed. |
| `skipped_reason` | string \| null | Stop/skip cause (`projection_breaker_open`, `projection_replay_locked`, `handoff_cli_unreachable`, `dead_letter_sink_full`, budget reasons, …). `null` means no skip **when a drain completed without a skip reason**. When `failed` is non-null and `attempted` is true, a `null` `skipped_reason` may also mean the skip field was never read (raise at the first `drain_result.get`) — treat as unknown, not "no skip" (L-09). Routed through the same completion-gated publisher as other drain fields (A-08). |
| `skipped_reason_unparseable` | bool \| null | Tri-state twin of `drained_unparseable` for the skip field (A-08 / REVD-R4-07). **`true`** when `drain_result.get("skipped_reason")` completed but the value is not a usable reason (empty string or non-string). **`false`** when the get completed with `None` (honest no-skip) or a non-empty string. **`null`** when the get never finished. Distinguishes the three meanings that would otherwise collapse into a single `skipped_reason: null`. |
| `failed` | string \| null | Single-token sanitised `Type:message` when the drain raised (including a pre-attempt orphan-probe raise); otherwise null. |
| `orphan_draining` | bool \| null | Post-drain pid-aware orphan probe. `null` when the probe itself raised. |

Consumers can distinguish clean / skip / failed / unknown reclaim without parsing stderr:
`attempted` + `ran` + `failed` + `skipped_reason` + `skipped_reason_unparseable` + `counters_unparseable`
(+ per-counter `drained_unparseable` / `pending_remaining_unparseable` + floor
`pending_remaining_capped_unparseable`).
A receipt with `drained=0, pending_remaining=0, counters_unparseable=false` is a
healthy attempted-zero. A degraded attempt publishes **null** on the unreadable
counter(s) with the matching `*_unparseable` flag true (and usually
`counters_unparseable=true`); the shape `drained=0, pending_remaining=0,
counters_unparseable=true` is unreachable after the type-strict publisher
because a true unparseable flag forces the corresponding numeric key to null
(OBS-08). A receipt with `drained=null` (and sibling counter nulls) means those
fields were never successfully read **or** completed as unreadable — do not
treat it as a confident zero drain.

**Normalization raise (REVA-R6-02):** after `_parse_strict_count` accepts a
value, a subsequent `int()` normalization that raises (hostile int subclass
`__int__`/`__index__`) is encoded as **never-read** for that counter
(`parse_completed` stays false; `*_unparseable` stays null; `failed` is
non-null). The type-check alone is not published as a completed determination.

### Threat model (unparseable-counter surface)

The only in-tree producer (`drain_spool` / `_drain_receipt`) always types
`drained` and `pending_remaining` as non-negative ints and
`pending_remaining_capped` as a real bool. The unparseable-counter / floor
surface exists for **out-of-tree / partial / hostile** producers (mocks, future
forks, hand-assembled receipts, subprocess envelopes that lose typing). Do not
delete it as dead code because the in-tree path never emits corrupt types
(REVF-SPOOL2-04).

## Human stderr line grammar

Space-delimited `key=value` fields, then an optional prose segment:

```
task-start: task_ref=… branch=… mode=… head=… projection=…
  [spool_depth=N|N+|unknown orphan_draining=true|false|unknown
   [drained=N|unknown] [pending_remaining=N|N+|unknown]
   [pending_remaining_capped=unread|unreadable]
   [counters_unparseable=true|false] [skipped_reason=…|unread|unreadable]
   [drain_failed=…]]
  [; <nuance>] [; run `make project-events-replay`]
```

Rules:

- Structured kv fields always precede prose (including `drain_failed=`).
- Depth / orphan / retain telemetry is emitted when `projection=pending` **or**
  a drain was attempted — not only on pending.
- `spool_depth` may be an exact integer, a saturated `N+` floor, or `unknown`
  when the probe raises.
- Receipt and human surfaces share one publisher for counters/bits: a value is
  published only when its conversion completed **and** the value is trustworthy
  (`_publish_drain_value` / `_human_*_token`). Incomplete reads and
  completed-unreadable values are `null` on the receipt; incomplete reads are
  omitted on the human line; completed-unreadable counters emit `unknown`.
  Direct (non-publisher) receipt fields are `failed`, `orphan_draining`,
  `attempted`, and the pre-reduced `counters_unparseable` aggregate.
- On a clean drain attempt (`drain_failed=` absent), every counter whose
  type-strict parse **completed as readable** is emitted (`drained=N`
  including healthy zero; `pending_remaining=N|N+` only when the floor bit
  also completed readable). A counter whose parse **completed as unreadable**
  surfaces as `drained=unknown` and/or `pending_remaining=unknown`. A counter
  whose parse **did not complete** is **omitted**. When remaining finished
  readable but the floor is unread or completed-unreadable, emit
  `pending_remaining=N` **and** a floor companion that distinguishes those
  two states: `pending_remaining_capped=unread` when the floor get never
  finished, `pending_remaining_capped=unreadable` when the get completed but
  the value is not a real bool (third form; rule b — do not withhold the
  magnitude, and do not claim exact/floor). Never collapse the two companions
  into a single token.
  The aggregate token is **two-state on the clean path**:
  `counters_unparseable=true` or `counters_unparseable=false`. A null aggregate
  only arises with an incomplete parse, which always sets `drain_error` and
  therefore leaves the clean path — the omit-null third state is unreachable
  under `drain_failed=` absent (REVE-R6-06). Emitting
  `counters_unparseable=false` on a healthy clean drain is intentional (A-07):
  operators and tests must distinguish confident-healthy from the mid-raise
  path where the aggregate token is absent.
- On a mid-raise path (`drain_failed=` set) emission is **per-counter**:
  - A counter whose type-strict parse **completed as readable** is still
    emitted alongside `drain_failed=` (`drained=N`, exact/`N+` remaining when
    the floor bit also completed readable; third form when floor unread).
  - A counter whose type-strict parse **completed as unreadable** still emits
    `unknown` alongside `drain_failed=` (a completed determination is not a
    default local). The aggregate `counters_unparseable=` stderr token
    remains clean-path only (absent under `drain_failed=`); under
    `drain_failed=` that omission carries **no** aggregate meaning. The
    receipt aggregate may still be `true` / `false` / `null` (S-04).
  - A counter whose parse **did not complete** is omitted entirely (default
    locals must not print as if they were read).
- Floor forms (only when remaining is readable): `N+` only when receipt
  `pending_remaining_capped` is `true`; exact `N` only when it is `false`
  (receipt `true` ↔ human `N+`; receipt `false` ↔ human exact `N`). When the
  floor bit is `null` and remaining is readable, human emits
  `pending_remaining=N` with a companion that mirrors the receipt pair:
  `pending_remaining_capped=unread` when the floor get never finished
  (`pending_remaining_capped_unparseable` is null), or
  `pending_remaining_capped=unreadable` when the get completed as non-bool
  (`pending_remaining_capped_unparseable` is true) — never falling through to
  exact N, never omitting the magnitude, and never collapsing those two
  companions. When remaining is unreadable, human emits
  `pending_remaining=unknown` and does not map the floor bit to N/N+ even if
  the receipt still carries a floor value.
- Retain prose must not name a count the kv token declined to state (A-04).
  When retain is known and the remaining token carries the magnitude, prose
  is `N entries retained` (floor known) or `N entries retained (floor unknown)`
  (third form). It must not claim the count itself is unknown when the
  receipt publishes it (rule b). Retained with no magnitude token is
  unreachable after third form (C-04).
- Prose nuance priority when a drain was attempted: unreadable counters →
  `spool counters unreadable` (and, when retain is also true, compose
  `; N entries retained` / `; N entries retained (floor unknown)` so
  retention does not vanish from the whole line — REVB-R6-03); else orphan →
  `a prior claim is orphaned`; else retained → `N entries retained` /
  `N entries retained (floor unknown)`; else pending projection →
  `queued, will reconcile`. Orphan stays visible via `orphan_draining=`;
  retained magnitude stays visible via third-form `pending_remaining=N` and
  the composed retain prose when unreadable wins the slot.
- `skipped_reason` on the human line mirrors the receipt's three rows with
  distinct tokens: a usable reason emits `skipped_reason=<token>`;
  completed-unusable emits `skipped_reason=unreadable`; never-read after an
  attempt emits `skipped_reason=unread`; honest no-skip omits the field.
  The two degraded arms must not re-collapse into a single token.
- `skipped_reason=projection_breaker_open` suppresses the
  `make project-events-replay` remediation (replay is what the breaker refuses).
  Completed-unusable and never-read-after-attempt also suppress remediation
  (they may encode a breaker-open stop; do not advise the refused command).
- `drain_failed=` values are a single token: whitespace → `_`, `=` → `:`,
  C0/C1/zero-width stripped, capped at 120 chars.
