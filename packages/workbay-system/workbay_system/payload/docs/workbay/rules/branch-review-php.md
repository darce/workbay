# Branch Review — PHP / Composer / PHPUnit

> Load this guide when the branch diff includes PHP source, Composer configuration, PHPUnit tests, or PHP framework integration files.
> For universal process, severity definitions, and report template see [branch-review-guide.md](branch-review-guide.md).

---

## Automated Checks

| Check           | Command source                                     |
| --------------- | -------------------------------------------------- |
| Static analysis | Repo-local Composer script, PHPStan, or Psalm command |
| Tests           | Repo-local Composer test script or PHPUnit command |

Require fresh PHPUnit + PHPStan evidence for runtime-sensitive fixes (bootstrap paths, controller composition, proxy/header forwarding, autoload behavior).

---

## Boundary and Runtime Correctness

- [ ] **Runtime bootstrap/autoload parity** — verify behavior under the real framework/Composer load path, not only the PHPUnit bootstrap fallback.
- [ ] **Adapter provenance** — controllers/adapters do not invent envelope fields (`limit`, `offset`, `total`, `data_source`, etc.); every field traces to the request, upstream payload, or documented local authority.
- [ ] **Header and status preservation** — proxy controllers preserve upstream HTTP status and headers without normalizing away failure semantics.
- [ ] **Degradation semantics** — empty, unavailable, and blocking are distinct outcomes; never silently conflated.

---

## Security

- [ ] **Input sanitization/validation** — untrusted request input is sanitized or validated against an allowlist before use.
- [ ] **One transport per parameter** — the same value is not read from multiple request sources (e.g. POST body and query params).
- [ ] **CSRF/anti-forgery verification** — all state-mutating endpoints verify a CSRF token or nonce.
- [ ] **Authorization checks** — privileged endpoints verify the caller's capability or role.

---

## SQL Bug-Finding Heuristics

### Data-flow through SQL binding

Verify the full chain per query: value origin → transformation → placeholder binding → DB interpretation.

- [ ] Placeholder count matches argument count (especially dynamic `$placeholders`).
- [ ] Arguments in correct positional order.
- [ ] Sentinel/default values survive binding. `'NULL'` (string) bound via `%s` → DB receives string `'NULL'`, not SQL `NULL`.
- [ ] `NULLIF()`, `COALESCE()`, `IF()` use the correct comparison value for the sentinel.

### Guard condition vs business rule alignment

State the business rule in plain language, then verify the SQL/code implements exactly that rule.

- [ ] Scope guards protect the **correct scope** — entity-level guards must not block related-entity operations.
- [ ] Deletion guards use the right operator — `NOT IN` vs `FIND_IN_SET` vs `NOT EXISTS` differ on NULL, empty sets, and multi-value strings.
- [ ] Early returns match their purpose — "empty input" returns must not skip cleanup that should always run.

### SQL function semantic correctness

- [ ] `FIND_IN_SET(col, %s)` — commas in data corrupt the set boundary. Prefer `NOT IN (...)` with individual placeholders.
- [ ] `GREATEST()` / `LEAST()` — any `NULL` argument → result is `NULL`.
- [ ] `IF(condition, a, b)` — condition must evaluate against **current** row state, not `VALUES()`.
- [ ] `ON DUPLICATE KEY UPDATE` — verify which fields refresh unconditionally vs guarded. Misguarding is a silent data bug.

### Boundary value sweep

For each function accepting numeric or collection inputs:

- [ ] **Empty** — empty array/string/zero: graceful degradation or invalid SQL / divide-by-zero?
- [ ] **Single element** — `implode()` valid? Loop body works on first-and-only iteration?
- [ ] **Large input** — 10k+ items: `NOT IN (...)` MySQL limits? Unbounded `LEFT JOIN` scan?

---

## Async Write Propagation (Outbox / Queue / Projection)

When the diff touches an outbox, projection, or async sync subsystem:

- [ ] **Projection atomicity** — projection writes commit inside a single transaction.
- [ ] **Outbox entry completeness** — every queued entry carries all required payload fields per the repo-local sync/queue contract.
- [ ] **Conflict reuse** — conflict records update existing open entries instead of accumulating duplicates.
- [ ] **State metrics updated** — projection, drain, retry/discard, and resolution paths refresh the relevant state repository.
- [ ] **Dead-letter status transitions** — retry: `failed` → `pending`; discard: `failed`/`conflict` → `discarded`.
