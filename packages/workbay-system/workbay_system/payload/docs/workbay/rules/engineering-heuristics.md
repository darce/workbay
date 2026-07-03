# Engineering Heuristics Lexicon

## About this document

> **Referenced, not read end-to-end.** This doc is the concept vocabulary for WorkBay skills and rule guides — like the Fowler smell table is for `refactor`. Activation surfaces inline the **cue** (trigger + question); this lexicon holds the **rationale**.

Each domain section is a `Trigger | Rule | Answers` table. Stable anchor slugs (`{#slug}`) support deep-links from lenses, checklists, and skill bodies.

## Resilience & Failure Modes

| Trigger | Rule | Answers |
| --- | --- | --- |
| Code retries a network/DB call, hedging, or at-least-once queue | **Retry without idempotency** — retried ops double-apply unless idempotent | Is this write safe to retry? |
| Connect/read/pool-checkout/HTTP client with no timeout | **Timeout on every blocking call** — every socket/pool/RPC/`wait()` needs a bounded timeout; defaults block forever | What bounds this wait? |
| Sync external call; only the error path handled | **Slow failure worse than fast failure** — a hung downstream ties up caller+callee worse than a refused connection | What happens when this dependency is slow, not down? |
| Resource checkout, try/finally close, pool sizing | **Resource-pool exhaustion / cascade** — downstream hang drains caller's pool; unreturned resource in `finally` leaks | Is a connection/thread leaked if this throws? |
| SELECT without LIMIT, fetch-all, master/detail traversal, collection API | **Unbounded result set** — query/response with no LIMIT can OOM in prod; dev data hides it | What if this returns 10M rows? |
| Retry loop with no delay/jitter/cap | **Aggressive retry storm / missing backoff** — tight retry against a struggling downstream amplifies the outage | Does this retry have backoff + ceiling? |
| Plan adds a table/log/session/cache that accumulates | **Steady-state reclaimer** — anything that grows needs same-rate purge | What purges this? |
| Plan adds cache/memoization | **Cache bound + invalidation** — unbounded cache = leak; miss cost = tail latency | Is the cache bounded and invalidated; miss-path latency? |
| High-fan-in front tier → smaller back tier | **Unbalanced capacities** — front out-generates back; promo load inverts safe ratios | What throttles the front when the back saturates? |
| Distributed locks/leases | **Fencing tokens** — paused/expired lease holder can write with stale state; storage must reject old monotonic tokens | Can a paused holder still write? |
| Plan accesses a table owned elsewhere | **Don't reach into another system's DB** — integration DB breaks encapsulation, couples consumers to internal schema | Does this read another service's DB directly? |
| Loop over remote/per-item RPCs | **Chatty remote interface (N+1)** — remote ≈ 1000× local; batch into coarse calls | Is this remote interface chatty? |
| Code crossing process/network/resource boundary | **Bugs are survived, not eliminated** — assume any external call eventually fails; design crumple zones | Ran the failure questions for every I/O point? |
| Fan-out/producer-consumer with no queue bound | **Backpressure via bounded queue** — bound producer/consumer with a full-queue policy | Where's the backpressure? |

## Concurrency & Async

| Trigger | Rule | Answers |
| --- | --- | --- |
| Blocking call inside `async def` with no `await`/executor | **Blocking the event loop** — sync I/O/CPU/sleep in a coroutine stalls every concurrent task | Is this async code blocking the loop? |
| Shared state touched across `await` | **Shared mutable state across await** — race window opens at every await; RMW spanning await needs lock/redesign | Can another task mutate this between read and write? |
| `except CancelledError`/bare except in a coroutine | **Swallowed CancelledError** — catching without re-raise/cleanup breaks cooperative shutdown | Does this coroutine honor cancellation? |
| `create_task()` result unassigned; gather without `return_exceptions` on shutdown | **Fire-and-forget / orphan task** — dropped task may be GC'd pending; exceptions lost | Is this background task tracked, or orphaned? |
| Read→compute→write-back; counters; check-then-act invariant | **Lost update / write skew** — concurrent RMW loses update under weak isolation | Do two concurrent writers need atomic update / CAS / serializable? |
| Converting sequential calls to `gather` | **Concurrency only helps independent I/O** — parallel/await cuts latency only when I/O overlaps | Will making this concurrent actually help? |

## Data & Consistency

| Trigger | Rule | Answers |
| --- | --- | --- |
| Plan adds replica/read-replica/cache/multi-region | **Consistency model must be stated** — declare strong/causal/session/eventual + stale-read tradeoff | What stale reads are acceptable? |
| Reads routed to async followers | **Replication-lag anomalies** — async replicas break read-your-writes / monotonic-read / causal order | Will a user see their own write; can reads go backward? |
| Change to wire format / DB / event schema / API | **Schema evolution compat** — rolling upgrades coexist old+new code; data outlives code | Can old code read new data and vice versa during rollout? |
| Schema/API change under no-downtime | **Expand→migrate→contract** — nullable columns, bridging, dual-version endpoints, then drop old | Does this migration keep N and N+1 running? |
| Atomic writes across 2+ stores; XA; global index | **Distributed-transaction / 2PC cost** — ~10× slower, blocks on coordinator failure | Does this need a distributed txn and can it tolerate the cost? |
| Distributed lock, leader election, hard-unique constraint | **Linearizability triggers consensus** — locks/leader-election need linearizability → consensus | Does this uniqueness/lock need real consensus? |
| Plan picks a partition/shard key | **Partition hot-spot / skew** — range keys and low-cardinality keys create hot partitions | Will this partition key hot-spot? |
| Secondary index on partitioned data | **Local vs global secondary index** — local = cheap writes/scatter-gather reads; global = fast reads/cross-partition writes | Which index partitioning fits the read/write mix? |
| Time/random/sequence/side-effecting call in replayed write path | **Non-deterministic replication/effects** — `NOW()`, `RAND()`, side effects diverge across replicas/retries | Same result on replay/replica/retry? |
| Writes behind async ack / write-behind cache | **Durability / data-loss tolerance** — async-ack writes lost on failover | Ok to lose last N seconds of writes on crash? |
| Large numeric IDs serialized to JSON/JS | **Large-int precision in JSON** — JSON/JS lose ints > 2^53 | Will this 64-bit ID survive JSON round-trip? |
| SQL via f-string/concat/format | **SQL parameterization** — always bind params; never interpolate values | Is this query parameterized? |

## Performance & Tail-Latency

| Trigger | Rule | Answers |
| --- | --- | --- |
| User-facing response-time expectation; any latency claim | **Percentiles, not averages** — target p95/p99/p999; means hide tails | What's the p99 target? |
| N parallel backend calls awaited together | **Tail-latency amplification** — end latency = slowest of N parallel sub-calls | Does this scatter/gather expose tail latency? |
| PR cites closed-loop benchmark / averages only | **Coordinated omission** — wait-then-send benchmarks undersample slow responses | Does this benchmark correct for coordinated omission? |
| Refactor proposes parallelism to cut latency | **Amdahl's law** — serial fraction caps speedup; compute ceiling before parallelizing | Max speedup given the serial part? |
| Hot-path nested iteration over runtime-sized collections | **Algorithmic complexity at scale** — O(n²) becomes a latency wall on hot paths | Does this nested loop blow up as n grows? |
| Any perf claim or premature-optimization urge | **Measure, don't guess (perf)** — profile first, tune hot spots after | Should I optimize this / is this the bottleneck? |
| Plan dismisses a per-request cost as negligible | **Capacity multiplier effects** — per-txn × volume and nonlinear costs | Per-txn cost × daily volume? |

## Refactoring & Design

| Trigger | Rule | Answers |
| --- | --- | --- |
| Caller switches on a bool/null return | **Deceptive booleans** — callee returns bool/`X\|null` the caller branch-checks → return an outcome enum | Why are null-checks piling up at this call site? |
| Each switch leg holds a block; new cases keep editing one fn | **Strategy-map over switch** — replace fat if/else-if with enum-keyed handler map | How to add a branch without editing existing code? |
| Same guard reused across handlers needing await + HTTP status | **Gate class** — reusable async precondition → class throwing typed exception caught by middleware | Where do repeated async permission checks belong? |
| 4+ conditions combined and reused | **Pipe / condition-object** — each boolean → `check()` combined via `.every()` | This && chain is huge and reused — extract to what? |
| Diff has new feature + large structural moves | **Two hats** — never mix new behavior with restructuring in one diff | Is this PR doing refactor + feature at once? |
| A getter also writes/mutates | **Command-Query Separation** — value-returning fn must have no observable side effect | Is this query secretly mutating? |
| Getter returns the backing array/map directly | **Encapsulate-collection leak** — raw collection lets callers mutate internals | Does this getter expose mutable internals? |
| Constructor takes raw values without validation | **Invalid-state-at-construction** — object constructible inconsistent | Can this object exist invalid? |
| A field mirrors other state | **Mutable/derived-data drift** — stored value computable from other data can desync | Can this cached/derived field go stale? |
| About to flag two similar blocks | **Coincidental vs real duplication** — don't DRY-merge code that won't change together | Is this duplication worth extracting? |
| About to flag a guard-clause fn | **Guard-clause multiple returns are fine** — don't flag early returns as single-exit violation | Are these early returns a problem? |
| Plan adds extension points/generic params with one caller | **YAGNI / speculative-generality gate** — reject hooks/abstract layers with one consumer | Is this plan over-abstracted? |
| Plan wires two modules together | **Coupling-type triage (Nygard)** — classify link: Operational / Developmental / Semantic / Functional / Incidental | What kind of coupling, and is it harmful? |
| Plan splits into services claiming independence | **Independent-deployability test** — unit ships only if tested without collaborators | Are these services actually decoupled? |
| Plan talks directly to an external lib/API/datastore | **Ports & adapters** — wrap out-of-scope dependency behind minimal adapter | Does this insulate against the 3rd party? |
| Plan surfaces transport/storage codes into domain logic | **Leaky abstraction** — reject abstractions leaking the wrong level | Does this leak transport/storage detail into the domain? |
| Reviewer tempted to flag "too many classes" | **Decoupling costs code; optimize for thinking** — extra structure may buy decoupling/clarity | Is this extra structure bloat or worth it? |

## Diagnosis Posture

| Trigger | Rule | Answers |
| --- | --- | --- |
| Debugging with a gut hypothesis | **Facts before hypothesis** — list known facts first; check hypothesis fits before acting | Am I acting on an assumed cause? |
| Reasoning from theory about behavior | **Empiricism** — model/experiment must match real conditions | Does my mental model match the real system? |
| Proposing a fix/approach | **Falsifiable hypothesis** — define how you'll evaluate before running | How will I know if I'm right? |
| Pressure to skip tests/refactoring | **No speed/quality trade-off** — disciplined quality is why high performers are fast | Cut quality to ship faster? |
| Choosing concise-obscure vs verbose-clear | **Optimize for thinking, not typing** — code is communication | Is this terse version actually better? |
| Naming/commenting | **Name after intent, not mechanism** — if you'd write a comment, write a named function | Does this name communicate intent? |
| Reasoning about blast radius | **Fault ≠ failure** — one component deviating becomes system failure when coupling spreads it | Where does this fault stop? |
| Judging production-readiness from local results | **QA ≠ production** — prod runs N:1 ratios, multi-node topology, prod-scale data | Does this only look safe at dev/QA scale? |
| Choosing an abstraction / evaluating against a ritual | **All models are wrong, some useful** — target abstractions to the problem; judge by results | Is this abstraction right-shaped; am I following process for its own sake? |
