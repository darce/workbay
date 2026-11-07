# Branch Review — Python / FastAPI / SQLAlchemy

> Load this guide when the branch diff includes Python service, package, migration, or test files.
> For universal process, severity definitions, and report template see [branch-review-guide.md](branch-review-guide.md).

---

## Automated Checks

Run the repo-local Python verification commands for the touched package or service. Prefer explicit environment prefixes or project-scoped runners over interactive activation when subprocess shells may lack shell init hooks.

| Check                    | Command source                                               |
| ------------------------ | ------------------------------------------------------------ |
| Lint + types + tests     | Package `Makefile`, `pyproject.toml`, or documented `pytest`/`ruff`/`mypy` commands |
| Cyclomatic complexity    | Repo-local `radon` command scoped to the touched package or service, when configured |

Require fresh evidence (`make check` or targeted `pytest` slice) for any claimed fix. Stale, missing, or partial evidence → `GAP` finding.

---

## Type Safety

- [ ] No `object` parameters — use domain types or Protocols.
- [ ] No `getattr()` + `callable()` guards — declare methods on Protocols.
- [ ] No `contextlib.suppress(Exception)` — catch specific exceptions, log.
- [ ] `assert` only for internal invariants/tests — use explicit exceptions for request/external-data validation.

---

## Architecture Boundaries

- [ ] **No raw SQL in the application layer** — `text()` calls only in `infrastructure/repositories/`.
- [ ] **No presentation DTOs in domain or application layer** — API response shapes in `interface_adapters/schemas/`.
- [ ] **No default-instantiating settings** — inject via DI, don't construct defaults inside functions.
- [ ] **No cross-layer exception duplication** — one canonical definition per exception.
- [ ] **No redundant router/dependency wiring** — each router registered exactly once.
- [ ] **No time-based gates on curated state** — gate on data deltas, never elapsed time.

---

## Boundary and Runtime Correctness

- [ ] **Dependency-injection parity** — test overrides match production dependency graph; no bypassing real startup or tenant/session behavior.
- [ ] **Session and transaction lifecycle parity** — tests verify the same commit/rollback/session-close semantics as production, not a looser autocommit path.
- [ ] **Schema and adapter validation parity** — Pydantic adapters validate against canonical boundary shapes; malformed payloads fail explicitly.
- [ ] **Degradation semantics stay explicit** — upstream failures, unavailable deps, and true empty results remain distinct outcomes.
- [ ] **Golden payload coverage for high-risk boundaries** — boundary shape changes have fixture or malformed-shape tests, not only happy-path assertions.

---

## Error Handling

- [ ] **LIKE wildcard escaping** — user-supplied strings in `ilike()` escape `%` and `_`.
- [ ] **No bare exception suppression** — `except Exception` logs at `WARNING` minimum.
- [ ] **Scoped exception clauses** — `try/except` wraps only the single operation it guards.
- [ ] **Consistent gate fallbacks** — all bypass paths apply the same checks (blocks AND constraints).

---

## Code Duplication

- [ ] **Shared repository utilities** — UUID coercion, media-identity bootstrap in `_helpers.py`.
- [ ] **Shared test stubs** — Protocol stubs used in 3+ files extracted to `tests/stubs.py`.
- [ ] **One canonical fake per protocol** — no divergent fakes across test files.
- [ ] **No duplicate methods** — Protocol interfaces have no aliased methods.

---

## Metric Thresholds

### Function Size

| Metric                  | Target | Max | Resolution                         |
| ----------------------- | ------ | --- | ---------------------------------- |
| Lines per function      | < 30   | 40  | Extract helper functions           |
| Parameters per function | < 5    | 7   | Use a parameter object / dataclass |

### Cyclomatic Complexity (radon)

| Grade   | Range | Action                                              |
| ------- | ----- | --------------------------------------------------- |
| **A**   | 1–5   | No action.                                          |
| **B**   | 6–10  | Acceptable; review for simplification.              |
| **C**   | 11–15 | **Requires justification.** Flag in review.         |
| **D**   | 16–20 | **Must refactor.**                                  |
| **E/F** | 21+   | **Block merge.**                                    |

Typical offenders: boundary converters, refresh orchestration, queue dispatch, and import/export pipelines.

### Performance Evidence

For changes touching DB queries, HTTP calls, background tasks, or queues, require evidence beyond unit-test pass/fail:

- query timing or request latency on the affected path
- connection-pool wait, retry, or queue behavior when applicable
- evidence distinguishing average improvements from tail-latency or saturation regressions

---

## Complexity Tooling

Add to `pyproject.toml`:

```toml
[project.optional-dependencies]
dev = [
    "radon>=6.0.0",
]
```

Makefile target:

```makefile
complexity:
	$(ACTIVATE) && python -m radon cc --min C --show-complexity --average src/
```
