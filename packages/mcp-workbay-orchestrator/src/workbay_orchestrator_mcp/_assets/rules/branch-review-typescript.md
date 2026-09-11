# Branch Review — TypeScript / React / Vitest

> Load this guide when the branch diff includes TypeScript, React, frontend package, or Vitest files.
> For universal process, severity definitions, and report template see [branch-review-guide.md](branch-review-guide.md).

---

## Automated Checks

| Check                   | Command source                                                                      |
| ----------------------- | ----------------------------------------------------------------------------------- |
| TypeScript types        | Repo-local `typecheck` script or package-specific `tsc --noEmit` command            |
| Tests                   | Repo-local Vitest/Jest test command scoped to the touched package                   |
| ESLint                  | Repo-local lint script or package-specific ESLint command                           |
| Architecture compliance | Repo-local architecture/design-system checker, when configured                      |

Require fresh evidence (`typecheck`, `lint`, targeted Vitest coverage) for any claimed UI fix. Screenshots, manual browsing, or stale output alone are insufficient.

---

## Type Safety

- [ ] No non-null assertions (`!`) on API data — use type guards.
- [ ] Assertion helpers (`asserts ...`), not `console.assert` — API/input validation stays explicit.
- [ ] No `undefined as T` or `x as T` casts — use union return types.
- [ ] No ad-hoc query keys — all keys through `queryKeys` factory.

---

## Frontend Patterns

- [ ] **No `!important` in SCSS** — increase specificity instead.
- [ ] **Design tokens for colors** — hex literals → CSS custom properties.
- [ ] **No inline styles for layout** — use SCSS classes.
- [ ] **API calls through API modules** — no direct `fetchApi` in components.
- [ ] **`URLSearchParams` for query strings** — no string interpolation.

---

## State Surface Correctness

- [ ] **UI state matrix** — changed surfaces cover empty, loading, error, degraded, and offline states.
- [ ] **Abort/cancel semantics** — expected cancellation produces no console warnings or error UI.
- [ ] **API-boundary payload validation** — components tolerate malformed JSON, partial payloads, missing optional fields without white-screen crashes.
- [ ] **Query invalidation regression** — post-mutation invalidation/refetch cannot silently restore stale UI state.

---

## Code Duplication

- [ ] **Shared algorithms** in reusable components, not inlined.
- [ ] **`retry: false`** in test QueryClients.
- [ ] **New component coverage** — render, loading, error, and primary interaction.

---

## SCSS Metric Thresholds

| Metric                                     | Threshold | Resolution                      |
| ------------------------------------------ | --------- | ------------------------------- |
| `!important` count                         | 0         | Increase selector specificity   |
| Raw hex colors (outside `var()` fallbacks) | 0         | Use the repo's design-token custom properties |
| Max selector nesting depth                 | 4         | Flatten or restructure          |

Component size limits and hook counts are enforced by `check-architecture-compliance.js`.

---

## Stateful UI (Overlay / Conflicts / Retry Queues)

When the diff touches overlay hooks, conflict-resolution surfaces, retry queues, or sync/status indicators:

- [ ] **URL param source of truth** — overlay visibility is driven by a query param via a dedicated hook, not ad-hoc React state.
- [ ] **No stale closure captures** — search-param setters use the functional updater form.
- [ ] **Query invalidation on mutation settle** — each mutation invalidates every dependent query key (e.g. the affected list, queue, and sync views), not just the mutated entity.
- [ ] **Status coverage** — the UI covers all documented health states (healthy, queued, stale, conflict, failure, offline/degraded) that exist in the local contract.
