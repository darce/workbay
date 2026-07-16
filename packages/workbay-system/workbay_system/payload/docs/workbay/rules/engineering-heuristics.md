# Engineering Heuristics Lexicon

## About this document

> **Referenced, not read end-to-end.** This doc is the concept vocabulary for WorkBay skills and rule guides — like the Fowler smell table is for `refactor`. Activation surfaces inline the **cue** (trigger + question); this lexicon holds the **rationale**.

Each domain section is a table: `ID | Trigger | Rule | Answers | T·P | Src`. **IDs are stable citation keys** — cite rules as `[RES-01]` in findings, plans, and skill bodies; IDs are immutable once assigned, and section headings are the deep-link anchors. `T·P` is tier·phase: tier **B**locker (correctness/data-loss/security if violated) / **S**hould (strong default, exemptions exist) / **J**udgment (weigh in context); phase **p**lan / **w**rite / **r**eview (where the rule primarily fires). `Src` names the distilled source (`literature/extracted/refactoring/distilled/<slug>.md`, chapter where known) — jump there for depth, micro-examples, and exemptions.

**Row contract** (for adding rules): the trigger must be observable in a diff, plan, error, or runtime signal; the rule must be falsifiable (condition → action → consequence); the question must be answerable from the diff+plan. Repo-derived rules enter at tier J with occurrence provenance and are promoted by the Short-Rules vote mechanism (`helpful=/harmful=`).

**Consumption contract**: `branch-review-guide` inlines review-phase cues; `planning-review-guide` inlines plan-phase cues; `reasoning-discipline` anchors on Diagnosis Posture and Agent Craft; `development-workflow` anchors on loop discipline. Skills filter by phase tag — never copy row bodies.

> _Synced from the heuristics canon by `make heuristics-sync` — edit the canon, not the book-derived rows here._

## Agent Craft

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| AGT-01 | session's first write lands within two turns on a task with prior state | **Orient before acting** — context, plan checklist, recent decisions, branch state: four reads prevent redo and collision | Did I check what's done and in flight before editing? | S·w | agentic-coding-heuristics ch-1 |
| AGT-02 | plan/finding/code cites a path or symbol not resolved this session | **No unresolved anchors** — grep/read before citing; a missing anchor is a finding, not a license to improvise | Did I verify this file/function exists as claimed? | B·w | agentic-coding-heuristics ch-2 |
| AGT-03 | fix claimed for a failure never reproduced in-session | **Make it fail before making it pass** — unverifiable fixes are labeled plausible, not done | Did I observe the failure, then observe its absence? | B·r | agentic-coding-heuristics ch-3 |
| AGT-04 | completion claim without command + decisive output line | **Evidence verbatim** — every done-claim carries the command and the line that proves it; also satisfies closure guards for free | What output line proves this claim? | S·r | agentic-coding-heuristics ch-3 |
| AGT-05 | diff hunk with no anchor in the task/checklist | **No while-I'm-here changes** — log the improvement as a next-action; keep the diff one-intent (see [REF-05]) | Can this diff's purpose be stated in one sentence? | S·r | agentic-coding-heuristics ch-4 |
| AGT-06 | discovery-requiring checklist items untouched while greppable ones complete | **Name the skip** — silent scope shrink presents 70% as 100%; declared gaps are recoverable | Which items did I not do, and did I say so? | S·r | agentic-coding-heuristics ch-4 |
| AGT-07 | same fact re-derived twice in one session | **Externalize at discovery** — notes/decisions written when learned survive compaction; working memory doesn't (see [NAME-06]) | Should this be in the log instead of my head? | S·w | agentic-coding-heuristics ch-5 |
| AGT-08 | same guard rejection hit twice without material change | **Rejection is specification** — satisfy the named requirement or escalate with evidence; never vary cosmetically, never bypass | What exactly is this rejection asking for? | B·w | agentic-coding-heuristics ch-6 |
| AGT-09 | deterministic error (schema/type/4xx) retried unchanged | **Don't retry determinism** — change something material or change strategy | What did I change since the last attempt? | S·w | agentic-coding-heuristics ch-6 |
| AGT-10 | except-and-continue with no durable record | **Degrade loudly** — a swallowed error that keeps the session alive must still land in a log (see [OBS-01]) | If this failure matters next week, where is it written? | S·r | agentic-coding-heuristics ch-6 |
| AGT-11 | operator asked a question the repo answers | **Grep before asking** — config, patterns, commands are self-serve; save the human round-trip for real forks | Can a tool call answer this? | S·w | agentic-coding-heuristics ch-7 |
| AGT-12 | third unproductive attempt at the same obstacle | **Timebox and hand off legibly** — tried/learned/next-imperative makes the handoff nearly free | Am I generating new information or variations? | S·w | agentic-coding-heuristics ch-7 |
| AGT-13 | implementing an alternative a recorded decision rejected | **Don't relitigate settled decisions** — record the objection, implement as decided, or stop and escalate; unilateral reversal breaks coordination | Does a recorded decision already answer this? | B·w | agentic-coding-heuristics ch-4 |

## Resilience & Failure Modes

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| RES-01 | Code retries a network/DB call, hedging, or at-least-once queue | **Retry without idempotency** — retried ops double-apply unless idempotent (see [DATA-13], [API-02]) | Is this write safe to retry? | B·w | release-it ch-5 |
| RES-02 | Connect/read/pool-checkout/HTTP client with no timeout | **Timeout on every blocking call** — every socket/pool/RPC/`wait()` needs a bounded timeout; defaults block forever. Exempt: in-process/in-memory calls | What bounds this wait? | B·w | release-it ch-5 |
| RES-03 | Sync external call; only the error path handled | **Slow failure worse than fast failure** — a hung downstream ties up caller+callee worse than a refused connection | What happens when this dependency is slow, not down? | S·r | release-it ch-5 |
| RES-04 | Resource checkout, try/finally close, pool sizing | **Resource-pool exhaustion / cascade** — downstream hang drains caller's pool; unreturned resource in `finally` leaks | Is a connection/thread leaked if this throws? | B·r | release-it ch-5 |
| RES-05 | SELECT without LIMIT, fetch-all, master/detail traversal, collection API | **Unbounded result set** — query/response with no LIMIT can OOM in prod; dev data hides it. Exempt: bounded-by-construction enums/config tables | What if this returns 10M rows? | B·w | release-it ch-4 |
| RES-06 | Retry loop with no delay/jitter/cap | **Aggressive retry storm / missing backoff** — tight retry against a struggling downstream amplifies the outage; never retry an unmodified 4xx (see [API-08]) | Does this retry have backoff + ceiling? | S·w | release-it ch-5 |
| RES-07 | Plan adds a table/log/session/cache that accumulates | **Steady-state reclaimer** — anything that grows needs same-rate purge, shipped in the same release | What purges this? | S·p | release-it ch-5 |
| RES-08 | Plan adds cache/memoization | **Cache bound + invalidation** — unbounded cache = leak; miss cost = tail latency (special case of [RES-07]; see [PERF-08]) | Is the cache bounded and invalidated; miss-path latency? | S·p | release-it ch-5 |
| RES-09 | High-fan-in front tier → smaller back tier | **Unbalanced capacities** — front out-generates back; promo load inverts safe ratios | What throttles the front when the back saturates? | S·p | release-it ch-4 |
| RES-10 | Distributed locks/leases | **Fencing tokens** — paused/expired lease holder can write with stale state; storage must reject old monotonic tokens | Can a paused holder still write? | B·p | designing-data-intensive-applications ch-8 |
| RES-11 | Plan accesses a table owned elsewhere | **Don't reach into another system's DB** — integration DB breaks encapsulation, couples consumers to internal schema (see [ARCH-02]) | Does this read another service's DB directly? | S·p | release-it ch-18 |
| RES-12 | Loop over remote/per-item RPCs | **Chatty remote interface (N+1)** — remote ≈ 1000× local; batch into coarse calls. Exempt: in-process calls | Is this remote interface chatty? | S·r | release-it ch-5 |
| RES-13 | Code crossing process/network/resource boundary | **Bugs are survived, not eliminated** — assume any external call eventually fails; design crumple zones (see [DIAG-07]) | Ran the failure questions for every I/O point? | S·r | release-it ch-3 |
| RES-14 | Fan-out/producer-consumer with no queue bound | **Backpressure via bounded queue** — bound producer/consumer with a full-queue policy; unbounded queues convert overload into latency growth then crash | Where's the backpressure? | B·r | release-it ch-5 |
| RES-15 | Circuit-less repeated calls to an external dependency | **Circuit-break integration points** — stop calling what's already failing; expose breaker state to operations | If this dependency starts timing out, what stops us hammering it? | S·p | release-it ch-5 |

## Concurrency & Async

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| CON-01 | py: Blocking call inside `async def` with no `await`/executor | **Blocking the event loop** — sync I/O/CPU/sleep in a coroutine stalls every concurrent task | Is this async code blocking the loop? | B·r | using-asyncio-in-python ch-3 |
| CON-02 | Shared state touched across `await` (py or TS) | **Shared mutable state across await** — race window opens at every await; RMW spanning await needs lock/redesign | Can another task mutate this between read and write? | B·r | using-asyncio-in-python ch-2 |
| CON-03 | py: `except CancelledError`/bare except in a coroutine | **Swallowed CancelledError** — catching without re-raise/cleanup breaks cooperative shutdown | Does this coroutine honor cancellation? | B·r | using-asyncio-in-python ch-3 |
| CON-04 | py: `create_task()` result unassigned; gather without `return_exceptions` on shutdown | **Fire-and-forget / orphan task** — dropped task may be GC'd pending; exceptions lost | Is this background task tracked, or orphaned? | S·r | using-asyncio-in-python ch-3 |
| CON-05 | Read→compute→write-back; counters; check-then-act invariant | **Lost update / write skew** — concurrent RMW loses update under weak isolation (see [DATA-14], [CON-11]) | Do two concurrent writers need atomic update / CAS / serializable? | B·r | designing-data-intensive-applications ch-7 |
| CON-06 | Converting sequential calls to `gather`/`Promise.all` | **Concurrency only helps independent I/O** — parallel/await cuts latency only when I/O overlaps (see [PERF-04]) | Will making this concurrent actually help? | J·w | using-asyncio-in-python ch-1 |
| CON-07 | ts: Promise-returning function using `.then` chains, or declared without `async` | **Async all the way** — `async` guarantees the Promise contract and never double-wraps | Is every Promise producer an `async` function? | S·w | effective-typescript ch-3 |
| CON-08 | ts: wrapper where one branch invokes the callback or resolves synchronously | **No half-synchronous functions** — a function must be always-sync or always-async; a sync fast path reorders callers' observable state | Do both branches cross the same microtask boundary? | B·r | effective-typescript ch-3 |
| CON-09 | Condition-variable / monitor wait guarded by `if` | **Wait in a while loop** — spurious wakeups make a wakeup a hint that state *might* have changed, never a guarantee | Is every wait inside a loop that re-tests the predicate? | B·r | os-three-easy-pieces ch-30 |
| CON-10 | Bare shared flag with spin or sleep-poll signaling between threads/tasks | **No ad-hoc synchronization** — ~half of studied flag-based syncs were buggy; use lock + state variable + condition/event primitive | Does this cross-thread signal use a real primitive? | B·r | os-three-easy-pieces ch-27 |
| CON-11 | Shared value checked in one step and used in another (null-check→deref, exists→open) | **Atomicity violation (check-then-act)** — the #1 real-world concurrency bug: the check is stale by the time the act runs | Is one lock/atomic op held across check and act — by every accessor? | B·r | os-three-easy-pieces ch-32 |
| CON-12 | Task B reads state task A initializes, with no join/CV/await edge between them | **Order violation** — "A always runs first" is scheduling luck, not synchronization; the #2 real-world concurrency bug | What edge guarantees the init happens-before this read? | B·r | os-three-easy-pieces ch-32 |
| CON-13 | Code path acquiring 2+ locks; same pair taken in different orders at different sites | **Order your locks** — circular wait is the one deadlock condition cheap to break; documented partial order | Can I state the global acquisition order, and does every site follow it? | B·p | os-three-easy-pieces ch-32 |
| CON-14 | New concurrent structure designed fine-grained/lock-free before contention measurement | **Big lock first** — the single-lock version is likely correct; refine only on measured contention (see [PERF-06]) | Is there contention data justifying more than one lock? | S·p | os-three-easy-pieces ch-29 |
| CON-15 | py: threads proposed for CPU-bound speedup, or thread-per-item fan-out | **Concurrency ladder (GIL)** — threads only overlap blocking I/O; pools for few/blocking, coroutines for many, processes for CPU | Is each unit I/O-bound or CPU-bound, and how many run at once? | S·p | effective-python ch-7 |
| CON-16 | py: 2+ threads read-modify-write shared state with no `Lock` | **The GIL is not your lock** — interpreter switches between bytecodes; `x += 1` interleaves and corrupts | Which lock serializes every mutation of this shared object? | B·r | effective-python ch-7 |
| CON-17 | Small concurrency test passes; loaded run fails by different amounts each time | **Nonreproducible-under-load = race** — treat as shared-state race, not flaky infrastructure | What shared state do workers RMW, and where can execution switch? | S·r | using-asyncio-in-python ch-2 |

## Data & Consistency

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| DATA-01 | Plan adds replica/read-replica/cache/multi-region | **Consistency model must be stated** — declare strong/causal/session/eventual + stale-read tradeoff. Exempt: single-node single-store designs | What stale reads are acceptable? | S·p | designing-data-intensive-applications ch-5 |
| DATA-02 | Reads routed to async followers | **Replication-lag anomalies** — async replicas break read-your-writes / monotonic-read / causal order | Will a user see their own write; can reads go backward? | S·p | designing-data-intensive-applications ch-5 |
| DATA-03 | Change to wire format / DB / event schema / API | **Schema evolution compat** — rolling upgrades coexist old+new code; data outlives code; both directions must read | Can old code read new data and vice versa during rollout? | B·r | designing-data-intensive-applications ch-4 |
| DATA-04 | Schema/API change under no-downtime | **Expand→migrate→contract** — nullable columns, bridging, dual-version endpoints, then drop old | Does this migration keep N and N+1 running? | S·p | release-it ch-18 |
| DATA-05 | Atomic writes across 2+ stores; XA; global index | **Distributed-transaction / 2PC cost** — ~10× slower, blocks on coordinator failure (see [ARCH-03]) | Does this need a distributed txn and can it tolerate the cost? | S·p | designing-data-intensive-applications ch-9 |
| DATA-06 | Distributed lock, leader election, hard-unique constraint | **Linearizability triggers consensus** — locks/leader-election need linearizability → consensus; coordinate only where apologies can't work | Does this uniqueness/lock need real consensus? | J·p | designing-data-intensive-applications ch-9 |
| DATA-07 | Plan picks a partition/shard key | **Partition hot-spot / skew** — range keys and low-cardinality keys create hot partitions; mod-N remaps everything on resize | Will this partition key hot-spot? | S·p | designing-data-intensive-applications ch-6 |
| DATA-08 | Secondary index on partitioned data | **Local vs global secondary index** — local = cheap writes/scatter-gather reads; global = fast reads/cross-partition writes | Which index partitioning fits the read/write mix? | J·p | designing-data-intensive-applications ch-6 |
| DATA-09 | Time/random/sequence/side-effecting call in replayed write path | **Non-deterministic replication/effects** — `NOW()`, `RAND()`, side effects diverge across replicas/retries (see [RES-01]) | Same result on replay/replica/retry? | B·r | designing-data-intensive-applications ch-11 |
| DATA-10 | Writes behind async ack / write-behind cache | **Durability / data-loss tolerance** — async-ack writes lost on failover | Ok to lose last N seconds of writes on crash? | S·p | designing-data-intensive-applications ch-5 |
| DATA-11 | Large numeric IDs serialized to JSON/JS | **Large-int precision in JSON** — JSON/JS lose ints > 2^53 | Will this 64-bit ID survive JSON round-trip? | B·r | designing-data-intensive-applications ch-4 |
| DATA-12 | SQL via f-string/concat/format | **SQL parameterization** — always bind params; never interpolate values (see [SEC-02]) | Is this query parameterized? | B·w | llm-security-playbook ch-7 |
| DATA-13 | Retryable operation crossing a network (RPC, POST, enqueue) | **End-to-end request ID** — TCP and DB transactions don't dedup across connections; a retried request double-executes without a client-generated ID hitting a UNIQUE constraint | Does a request ID travel to the final store? | B·w | designing-data-intensive-applications ch-12 |
| DATA-14 | Code writes the same logical change to two stores (DB + index/cache) | **No dual writes** — without a single order authority the copies diverge permanently and silently; derive the second from the first's change log | Which system is the system of record? | B·p | designing-data-intensive-applications ch-11 |
| DATA-15 | Wall-clock timestamps used to order events or resolve conflicts | **Logical clocks for ordering** — clock skew makes LWW drop causally-later writes with no error | Should this be a counter/version vector? | B·w | designing-data-intensive-applications ch-8 |
| DATA-16 | File created/updated for durability with no `fsync`, or new file without directory fsync | **write() is not durability** — buffered data dies with a crash; a created file isn't reachable until its directory entry is forced; atomic replace = write-temp, fsync, rename | After the crash we defend against, is content AND directory entry on disk? | B·r | os-three-easy-pieces ch-39 |

## Performance & Tail-Latency

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| PERF-01 | User-facing response-time expectation; any latency claim | **Percentiles, not averages** — target p95/p99/p999; means hide tails; percentiles don't average across hosts | What's the p99 target? | S·r | latency-reduce-delay-in-software-systems ch-2 |
| PERF-02 | N parallel backend calls awaited together | **Tail-latency amplification** — end latency = slowest of N; 1% slow × fanout 100 ⇒ 63% of users hit the tail | Does this scatter/gather expose tail latency? | S·p | latency-reduce-delay-in-software-systems ch-2 |
| PERF-03 | PR cites closed-loop benchmark / averages only | **Coordinated omission** — wait-then-send benchmarks undersample slow responses | Does this benchmark correct for coordinated omission? | B·r | latency-reduce-delay-in-software-systems ch-2 |
| PERF-04 | Refactor proposes parallelism to cut latency | **Amdahl's law** — serial fraction caps speedup; compute ceiling before parallelizing; benchmark the single thread first | Max speedup given the serial part? | S·p | latency-reduce-delay-in-software-systems ch-9 |
| PERF-05 | Hot-path nested iteration over runtime-sized collections | **Algorithmic complexity at scale** — O(n²) becomes a latency wall on hot paths (see [ALG-02]) | Does this nested loop blow up as n grows? | S·r | algorithm-design-manual ch-2 |
| PERF-06 | Any perf claim or premature-optimization urge | **Measure, don't guess (perf)** — profile first, tune hot spots after; back out changes with no measured win | Should I optimize this / is this the bottleneck? | S·r | philosophy-of-software-design ch-20 |
| PERF-07 | Plan dismisses a per-request cost as negligible | **Capacity multiplier effects** — per-txn × volume and nonlinear costs | Per-txn cost × daily volume? | S·p | release-it ch-4 |
| PERF-08 | Cache proposed to meet a latency SLO | **Caches fix averages, not tails** — p99 stays at miss latency; pair with a miss-cost plan and size ≥ working set | What does p99 look like on a miss? | S·p | latency-reduce-delay-in-software-systems ch-6 |
| PERF-09 | Latency target set for a new endpoint | **Budget against latency constants** — each physical boundary crossing costs ~an order of magnitude; geography sets a hard floor | Do the constants permit the target? | S·p | latency-reduce-delay-in-software-systems ch-1 |
| PERF-10 | Sequential awaits over independent I/O calls | **Join independent waits** — latency of independent ops is their max, not their sum | Are these awaited operations actually dependent? | S·w | latency-reduce-delay-in-software-systems ch-9 |

## Refactoring & Design

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| REF-01 | Caller switches on a bool/null return | **Deceptive booleans** — callee returns bool/`X\|null` the caller branch-checks → return an outcome enum | Why are null-checks piling up at this call site? | S·w | refactoring-typescript ch-5 |
| REF-02 | Each switch leg holds a block; new cases keep editing one fn | **Strategy-map over switch** — replace fat if/else-if with enum-keyed handler map. Exempt: a single switch in one place is fine | How to add a branch without editing existing code? | J·w | refactoring-typescript ch-5 |
| REF-03 | Same guard reused across handlers needing await + HTTP status | **Gate class** — reusable async precondition → class throwing typed exception caught by middleware | Where do repeated async permission checks belong? | J·p | refactoring-typescript ch-4 |
| REF-04 | 4+ conditions combined and reused | **Pipe / condition-object** — each boolean → `check()` combined via `.every()` | This && chain is huge and reused — extract to what? | S·w | refactoring-typescript ch-3 |
| REF-05 | Diff has new feature + large structural moves | **Two hats** — never mix new behavior with restructuring in one diff | Is this PR doing refactor + feature at once? | S·r | refactoring-fowler-beck ch-2 |
| REF-06 | A getter also writes/mutates | **Command-Query Separation** — value-returning fn must have no observable side effect | Is this query secretly mutating? | S·r | refactoring-fowler-beck ch-11 |
| REF-07 | Getter returns the backing array/map directly | **Encapsulate-collection leak** — raw collection lets callers mutate internals | Does this getter expose mutable internals? | S·r | refactoring-fowler-beck ch-7 |
| REF-08 | Constructor takes raw values without validation | **Invalid-state-at-construction** — object constructible inconsistent (see [DOM-05]) | Can this object exist invalid? | S·w | refactoring-typescript ch-5 |
| REF-09 | A field mirrors other state | **Mutable/derived-data drift** — stored value computable from other data can desync; compute on demand | Can this cached/derived field go stale? | S·r | refactoring-fowler-beck ch-9 |
| REF-10 | About to flag two similar blocks | **Coincidental vs real duplication** — don't DRY-merge code that won't change together; DRY stops at the pipeline boundary | Is this duplication worth extracting? | J·r | refactoring-fowler-beck ch-3 |
| REF-11 | About to flag a guard-clause fn | **Guard-clause multiple returns are fine** — don't flag early returns as single-exit violation | Are these early returns a problem? | J·r | refactoring-typescript ch-4 |
| REF-12 | Plan adds extension points/generic params with one caller | **YAGNI / speculative-generality gate** — reject hooks/abstract layers with one consumer; prefer YAGNI unless ≥2 concrete consumers exist today (tension: [REF-17]) | Is this plan over-abstracted? | S·p | refactoring-fowler-beck ch-3 |
| REF-13 | Plan wires two modules together | **Coupling-type triage (Nygard)** — classify link: Operational / Developmental / Semantic / Functional / Incidental | What kind of coupling, and is it harmful? | S·p | release-it ch-18 |
| REF-14 | Plan splits into services claiming independence | **Independent-deployability test** — unit ships only if tested without collaborators (see [ARCH-01]) | Are these services actually decoupled? | S·p | modern-software-engineering ch-13 |
| REF-15 | Plan talks directly to an external lib/API/datastore | **Ports & adapters** — wrap out-of-scope dependency behind minimal adapter; every hard-coded library call is a forfeited seam | Does this insulate against the 3rd party? | S·p | modern-software-engineering ch-12 |
| REF-16 | Plan surfaces transport/storage codes into domain logic | **Leaky abstraction** — reject abstractions leaking the wrong level (see [API-10]) | Does this leak transport/storage detail into the domain? | S·r | philosophy-of-software-design ch-5 |
| REF-17 | Reviewer tempted to flag "too many classes" | **Decoupling costs code; optimize for thinking** — extra structure may buy decoupling/clarity | Is this extra structure bloat or worth it? | J·r | philosophy-of-software-design ch-4 |
| REF-18 | New class/method whose interface description ≈ its implementation | **Deep modules** — a module's cost is its interface, its benefit the functionality hidden behind it | Is this interface much simpler than what it hides? | S·p | philosophy-of-software-design ch-4 |
| REF-19 | Design decision (format, protocol, representation) appearing in >1 module | **No information leakage** — every leaked decision couples modules so a change fans out invisibly | Which single class could own this knowledge outright? | S·r | philosophy-of-software-design ch-5 |
| REF-20 | New throw/error for a condition callers will routinely catch-and-ignore | **Define errors out of existence** — respecify so the case is normal behavior; most catastrophic distributed failures are error-handling bugs | Can the spec make this a successful no-op or clamped result? | S·w | philosophy-of-software-design ch-10 |
| REF-21 | New config parameter, or exception punted to callers, in lieu of an internal decision | **Pull complexity downward** — the module implementer should suffer so its many users don't | Could this module handle this better than its users can? | S·p | philosophy-of-software-design ch-8 |
| REF-22 | Method that only forwards its arguments to a near-identical signature | **No pass-through methods** — adjacent layers must offer different abstractions | What distinct responsibility does each layer own? | S·r | philosophy-of-software-design ch-7 |
| REF-23 | Bug-fix/feature diff adding a kludge or special case | **Stay strategic** — after the change the system should look designed that way from the start; preparatory refactoring first | What would this look like if planned originally? | S·w | philosophy-of-software-design ch-16 |
| REF-24 | Diff adds an optional/defaulted param that changes a public method's behavior | **No behavior-flag params** — one named public method per behavior; `f(x, false)` hides the API surface | Does this param select *what the method does* rather than supply data? | S·r | refactoring-fowler-beck ch-11 |

## Testing Strategy

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| TEST-01 | Plan touches code without a fast self-checking test suite | **Tests before refactoring** — refactoring without a bug detector converts small mistakes into long debugging sessions | Can a suite run in seconds and catch a behavior change here? | B·p | refactoring-fowler-beck ch-4 |
| TEST-02 | Any edit planned to code with no tests around the change area | **Legacy change algorithm** — identify change points → find test points → break dependencies → write tests → change | Which step of the algorithm are we on? | S·p | working-effectively-with-legacy-code ch-2 |
| TEST-03 | Diff modifies behavior of an untested function/class | **Characterization before change** — pin actual current behavior first; actual, not intended, is the contract | Does a test document what this code does *today*? | S·w | working-effectively-with-legacy-code ch-13 |
| TEST-04 | Plan needs to fake a collaborator but no substitution point exists | **Find the seam** — create an enabling point (parameter, factory, getter); prefer object seams | Where can test and production choose different behavior? | S·p | working-effectively-with-legacy-code ch-4 |
| TEST-05 | New feature being inlined into a large untested method | **Sprout, don't inline** — write new code as a TDD'd method/class called from the untested host | Can this be new, separately tested code instead? | S·w | working-effectively-with-legacy-code ch-6 |
| TEST-06 | New test about to be run for the first time | **Watch it fail once / predict the failure** — a test never observed failing (with the predicted message) may assert nothing | What exact failure message do you expect before you run it? | S·w | modern-software-engineering ch-8 |
| TEST-07 | Test fixture built in shared scope and mutated by test bodies | **Fresh fixture per test** — shared mutable fixtures produce order-dependent intermittent failures | Is any object created once, reused, and mutated across tests? | B·r | refactoring-fowler-beck ch-4 |
| TEST-08 | Test suite gives different results for the same code version | **Determinism is non-negotiable** — non-deterministic evaluation certifies nothing; isolate or design the concurrency out to the edges | Same version, N runs: identical results every time? | B·r | modern-software-engineering ch-9 |
| TEST-09 | A test is hard to write, needs threads, or needs a real DB/file/network | **Hard test = design defect** — test difficulty is the earliest objective signal of coupling problems; and it's not a unit test — keep it out of the fast suite | What coupling makes this hard — can a seam remove it? | S·w | modern-software-engineering ch-14 |
| TEST-10 | Plan's primary verification is end-to-end tests across services | **E2E is a supplement, not a strategy** — composite tests can't express failure-injection and fail unattributably | Which module interface could measure this instead? | S·p | modern-software-engineering ch-9 |
| TEST-11 | Test coverage appears as a target or incentive | **Measure stability, not coverage** — coverage is gameable; change-failure rate measures what you want | Does every test assert the behavior its name claims? | S·r | modern-software-engineering ch-8 |
| TEST-12 | Test targets a private method | **Private-method test urge = SRP signal** — test via the public interface, or move the method where it's legitimately public | Testing problem, or class-doing-too-much problem? | J·r | working-effectively-with-legacy-code ch-10 |
| TEST-13 | py: `Mock()` created without `spec=`; or tests stacking `patch()` contexts | **Spec your mocks, inject your dependencies** — spec-less mocks accept misspelled methods; prefer injection over patching internals | Would calling a nonexistent method on this mock fail the test? | S·w | effective-python ch-9 |
| TEST-14 | Reviewer flags "ugly" test-enabling code in a dependency-breaking commit | **Incision-point exemption** — conservative ugliness that gets code under test is sanctioned; cleanup after tests exist | Is this the incision that enables tests, with a follow-up path? | J·r | working-effectively-with-legacy-code ch-25 |

## Debugging Procedure

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| DBG-01 | Fix or diff proposed before the failure has been reproduced | **Make It Fail** — an unreproduced bug can't be observed failing; the fix can't be verified, only lucky | Can I run one recorded sequence right now that demonstrates the failure? | B·p | debugging-9-rules ch-4 |
| DBG-02 | Root-cause claim with no attached observation (log line, trace, good-vs-bad diff) | **Quit Thinking and Look** — guessed mechanisms fix something that isn't broken and mask the real bug | What did I actually see failing, as opposed to infer? | B·r | debugging-9-rules ch-5 |
| DBG-03 | Diff bundles multiple speculative changes "to see if it helps" | **Rifle, Not Shotgun** — a multi-change success leaves the cause unknown; change one thing at a time | Which single variable does this experiment test? | S·w | debugging-9-rules ch-7 |
| DBG-04 | An experimental change that didn't fix the bug remains in the tree | **Back Out the Non-Fix** — a no-effect change still changed behavior and corrupts verification of the real fix | Have I reverted every disproven experiment? | B·w | debugging-9-rules ch-7 |
| DBG-05 | Internals investigated before verifying foundations (right binary/branch, service up, config present) | **Check the Plug** — foundation violations look otherworldly while you debug details | Am I certain I'm running the code and config I think I am? | S·p | debugging-9-rules ch-9 |
| DBG-06 | "Fixed" declared without re-running the original failing sequence | **If You Didn't Fix It, It Ain't Fixed** — testing under different conditions proves nothing about the bug you had; removing only the fix should bring the failure back | Did the exact repro that failed before now pass? | B·r | debugging-9-rules ch-11 |
| DBG-07 | Issue closed as "can't reproduce anymore" with no cause found | **It Never Goes Away by Itself** — the conditions changed, not the bug; it returns in the field | What trap did I leave to capture it when it recurs? | B·r | debugging-9-rules ch-11 |
| DBG-08 | Failing input or repro recipe is large and mostly irrelevant | **Minimize the failing case (ddmin)** — shrink until every remaining element is failure-relevant | Does removing any single remaining piece make the failure disappear? | S·w | why-programs-fail ch-5 |
| DBG-09 | "Worked in version X, fails in Y" with many intervening changes | **Bisect the history, don't read the diff** — an automated test over ordered changes finds the culprit in O(log n) | Is there an automated test both endpoints can run? | S·p | why-programs-fail ch-13 |
| DBG-10 | Fix special-cases the failing value or symptom (`if (x == badcase)`) | **Fix the cause, not the symptom** — symptom patches leave the defect live; the fix must break the defect→infection→failure chain; then sweep for sibling defects | Can we state how this change breaks the infection chain? | B·r | why-programs-fail ch-15 |
| DBG-11 | Diagnosis blames the last warning/change/anomaly seen before the failure | **Prove the cause by removing it** — a cause is established only when its absence makes the failure vanish; precedence is not causation | Did an experiment show the failure disappears without this candidate? | B·r | why-programs-fail ch-12 |
| DBG-12 | Escalation/handoff opens with a theory instead of symptoms and conditions | **Report Symptoms, Not Theories** — transmitted theories drag the helper into your rut | Deleted my hypothesis — does the report still describe what was observed? | S·r | debugging-9-rules ch-10 |
| DBG-13 | Failure vanishes when logging/debugger/timing changes | **Heisenbug means undefined behavior or a race** — suspect uninitialized state or races; confirm observations by two independent means | What unchecked nondeterminism could make observation mask this failure? | J·p | why-programs-fail ch-4 |

## Diagnosis Posture

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| DIAG-01 | Debugging with a gut hypothesis | **Facts before hypothesis** — list known facts first; check hypothesis fits before acting (procedure: [DBG-01]..[DBG-13]) | Am I acting on an assumed cause? | S·r | modern-software-engineering ch-7 |
| DIAG-02 | Reasoning from theory about behavior | **Empiricism** — model/experiment must match real conditions | Does my mental model match the real system? | S·r | modern-software-engineering ch-2 |
| DIAG-03 | Proposing a fix/approach | **Falsifiable hypothesis** — define how you'll evaluate before running | How will I know if I'm right? | S·w | why-programs-fail ch-6 |
| DIAG-04 | Pressure to skip tests/refactoring | **No speed/quality trade-off** — disciplined quality is why high performers are fast | Cut quality to ship faster? | S·p | modern-software-engineering ch-1 |
| DIAG-05 | Choosing concise-obscure vs verbose-clear | **Optimize for thinking, not typing** — code is communication | Is this terse version actually better? | S·w | modern-software-engineering ch-10 |
| DIAG-06 | Naming/commenting | **Name after intent, not mechanism** — if you'd write a comment, write a named function (see [NAME-01]) | Does this name communicate intent? | S·w | refactoring-fowler-beck ch-3 |
| DIAG-07 | Reasoning about blast radius | **Fault ≠ failure** — one component deviating becomes system failure when coupling spreads it (see [RES-13]) | Where does this fault stop? | S·r | release-it ch-3 |
| DIAG-08 | Judging production-readiness from local results | **QA ≠ production** — prod runs N:1 ratios, multi-node topology, prod-scale data | Does this only look safe at dev/QA scale? | S·r | release-it ch-4 |
| DIAG-09 | Choosing an abstraction / evaluating against a ritual | **All models are wrong, some useful** — target abstractions to the problem; judge by results | Is this abstraction right-shaped; am I following process for its own sake? | J·r | modern-software-engineering ch-2 |

## Observability

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| OBS-01 | New endpoint/queue/external call in a diff, no telemetry | **Instrument at write time** — uninstrumented paths are invisible in production; bugs cost most after intent fades | Does this diff emit an event/span for the work it adds? | S·w | observability-engineering ch-11 |
| OBS-02 | Multiple log lines per request; free-text logs | **One wide event per unit of work** — unstructured narratives can't be sliced, grouped, or traced | Can this request's story be queried as one structured record with a trace ID? | S·w | observability-engineering ch-5 |
| OBS-03 | Log line lacks a request/session/transaction identifier | **Correlation ID on every line** — post-mortems are greps | Can one grep reconstruct this transaction's path? | S·w | release-it ch-17 |
| OBS-04 | Log statement at ERROR level for business noise | **ERROR means operator action** — false positives train operators to ignore real alarms | Would on-call need to act on this at 3 a.m.? | S·w | release-it ch-17 |
| OBS-05 | New pool, cache, breaker, or gateway without metrics | **Expose everything, externalize policy** — emit state, counts, high-water marks; thresholds live outside the app | What does ops see when this misbehaves — without a redeploy? | S·w | release-it ch-17 |
| OBS-06 | Telemetry drops user/request/build IDs as "too unique" | **Keep high cardinality** — unique IDs are the most valuable debugging keys; bucket down later, never up | Is a high-cardinality field being omitted to appease a metrics backend? | S·r | observability-engineering ch-1 |
| OBS-07 | Paging alert on CPU/memory/disk threshold | **Page on symptoms, not causes** — resource thresholds have benign explanations; alert on degraded user experience | Does this alert indicate real degradation AND have a non-rote response? | S·r | observability-engineering ch-12 |
| OBS-08 | An instrumented table/log stream goes quiet | **Silence is not success** — dead instrumentation must break loudly (a freshness gate), not read as health | Can we distinguish "no events" from "capture broken"? | S·p | observability-engineering ch-8 |

## Security

> **Moved to its own lexicon.** The LLM and agent-security rules `SEC-01..10` now live in [`security-heuristics.md`](security-heuristics.md), beside the web, PHP, WordPress, and PostgreSQL security rules. Cite them by ID exactly as before (`[SEC-04]`); the IDs are unchanged, and inline cross-references (`see [SEC-02]`) still resolve. Security became a spanning concern once this canon grew a database and web surface, the same reason accessibility has its own lexicon.

## API Design

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| API-01 | Endpoint returns a collection with no record limit or paging | **Bound every collection** — documented default max, clamped client limits, next/prev links (see [RES-05]) | Can this response grow without bound, and could a client tell it was truncated? | S·p | restful-web-api-patterns ch-6 |
| API-02 | Non-idempotent POST (create, payment, send) retried by client, gateway, or queue | **No unsafe retry without idempotency** — a lost response plus blind retry double-executes; idempotency key or conditional PUT (see [DATA-13]) | If the response is lost, can this request be safely re-sent? | B·p | restful-web-api-patterns ch-3 |
| API-03 | Write body expresses a relative change ("increment", "apply 5%") | **Replacement over increment** — replay-safe writes state current and new values; deltas corrupt on retry | If this write ran twice, would the result differ? | B·w | restful-web-api-patterns ch-3 |
| API-04 | Update handler writes without a version/ETag precondition | **Conditional writes** — `If-Match` + 412 prevents lost updates between concurrent writers | What happens if two clients update this concurrently? | S·r | restful-web-api-patterns ch-6 |
| API-05 | API error returned as ad-hoc JSON, bare status, or stack trace | **Problem-details envelope (RFC 7807)** — machine-actionable errors; debug internals never in the envelope | Would a machine client act on this error without parsing prose? | S·w | restful-web-api-patterns ch-5 |
| API-06 | Filter/search endpoint returns 404 for an empty result set | **Empty is 200** — an empty match is a successful query; 404 is for a missing directly-addressed resource | Is "no matches" an error here, or an answer? | S·r | restful-web-api-patterns ch-6 |
| API-07 | Request handler performs work that can exceed a few seconds synchronously | **202 + status resource** — acknowledge immediately, expose poll/cancel links; explicit delay beats timeout roulette | Under worst-case volume, how long does this handler hold the connection? | S·p | restful-web-api-patterns ch-7 |
| API-08 | Retry helper with fixed interval, unbounded attempts, or retry-on-4xx | **Backoff, bounded, 5xx-only** — exponential backoff, ~3 attempts, never retry an unmodified 4xx (see [RES-06], [AGT-09]) | Which failure classes does this retry, and what stops it? | S·r | restful-web-api-patterns ch-7 |
| API-09 | Diff removes, renames, retypes, or makes-required an element of a published API | **Don't change it, add it** — take nothing away, redefine nothing, additions optional; changed defaults count as breaking (Hyrum's Law) | Could any existing caller observe this change? | B·r | restful-web-api-patterns ch-2 |
| API-10 | API resource shapes mirror DB tables, ORM entities, or internal names | **Interface is its own artifact** — translate internal models at the boundary so storage refactors never break callers (see [REF-16]) | If the storage schema changed tomorrow, would this API change too? | S·p | restful-web-api-patterns ch-5 |
| API-11 | Service persists or forwards a record after dropping unrecognized fields | **Must Ignore, round-trip whole** — ignore unknown fields on read but preserve them on write; stripping destroys other services' data through you | Does this write path preserve fields this service doesn't understand? | B·r | restful-web-api-patterns ch-6 |

## Domain Modeling

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| DOM-01 | New module/service planned without a stated subdomain type | **Classify before you architect** — core/supporting/generic determines pattern and build-vs-buy | Is this a differentiator, plumbing, or a solved problem? | S·p | learning-domain-driven-design ch-1 |
| DOM-02 | Aggregates/event sourcing/CQRS proposed for CRUD-simple logic | **Don't DDD-ify supporting subdomains** — transaction script/active record is *correct* for simple logic; elaborate patterns there are accidental complexity | Would active record fully express these rules? | S·p | learning-domain-driven-design ch-10 |
| DOM-03 | Same term bound to two meanings, or two names for one concept, in one module | **One meaning per term per context** — conflicting models must split into bounded contexts; context prefixes in names reveal a hidden boundary | Which context does each meaning belong to? | S·r | learning-domain-driven-design ch-3 |
| DOM-04 | Diff commits changes to two aggregate roots in one transaction | **Transaction boundary = aggregate boundary** — needing multi-aggregate commits is proof the boundaries are wrong; reference other aggregates by ID only | What is the smallest data set that must be strongly consistent? | B·w | learning-domain-driven-design ch-6 |
| DOM-05 | Money, IDs, emails, units passed around as primitives with repeated validation | **Value objects over primitive obsession** — immutable, self-validating types centralize the logic; always for money (see [REF-08]) | Where else is this value validated or manipulated? | S·w | learning-domain-driven-design ch-6 |
| DOM-06 | `publish(event)` inside an aggregate method or after the DB commit | **Outbox or it didn't happen** — pre-commit publish leaks uncommitted state; post-commit publish is lost on crash | Is the event's persistence atomic with the state change? | B·r | learning-domain-driven-design ch-9 |
| DOM-07 | Event sourcing proposed (or resisted) for a module | **Event sourcing must earn its cost** — adopt for money/audit/deep-analytics needs; otherwise dead weight | Does the business need the history, or just the state? | J·p | learning-domain-driven-design ch-7 |
| DOM-08 | Consumers subscribing to another service's internal event stream | **Publish a consumer contract, not your internals** — translate to a published language of public events | Would an internal schema change break this consumer? | S·p | learning-domain-driven-design ch-15 |

## Architecture & Trade-offs

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| ARCH-01 | Plan splits a service into smaller services | **Disintegrators need integrators** — a split is justified only by measured drivers (volatility, scale delta, isolation) that survive the integrator check (shared transaction, chatty workflow, entangled tables) | Which measured disintegrator applies, and which integrator did you rule out? | S·p | architecture-hard-parts ch-7 |
| ARCH-02 | Plan lets two or more services write the same table | **Single writer owns the table** — multi-writer tables break bounded contexts (see [RES-11]) | Which one service owns writes, and how do others submit changes? | S·p | architecture-hard-parts ch-9 |
| ARCH-03 | Plan assumes an all-or-nothing transaction spanning services | **Cross-service = BASE, not ACID** — distributed transactions lose atomicity and isolation; design the failure path or merge the services (see [DATA-05]) | What happens when the second write fails after the first committed? | B·p | architecture-hard-parts ch-9 |
| ARCH-04 | Plan models a workflow with several error paths as choreography | **Orchestrator utility rises with complexity** — error paths add links to choreography but reuse orchestration's; one orchestrator per workflow, never global | How many error scenarios, and where does each one's handling live? | J·p | architecture-hard-parts ch-11 |
| ARCH-05 | Plan puts domain logic in a sidecar, shared platform layer, or volatile shared library | **Reuse is operationalized by slow rate of change** — volatile shared domain assets ripple to every consumer | How often does this shared asset change, and who redeploys when it does? | S·p | architecture-hard-parts ch-8 |
| ARCH-06 | Design/plan presents a chosen approach with no downsides listed | **Everything is a trade-off** — if you haven't found the downside, you haven't found it yet; seek least-worst, not best | What does this choice sacrifice, and who confirmed that's acceptable? | S·r | architecture-hard-parts ch-1 |
| ARCH-07 | Consequential architecture decision lands without recorded rationale | **Why beats how (write the ADR)** — context, decision, consequences must outlive the author | Where is the record with alternatives considered and trade-offs accepted? | S·r | architecture-hard-parts ch-1 |
| ARCH-08 | New shard/microservice/datastore while a boring stack still fits | **Choose boring technology / single machine first** — well-understood edge cases beat novel infrastructure; if data fits one machine it usually outperforms the cluster | What problem does this addition solve that the boring option cannot? | J·p | observability-engineering ch-3 |

## Algorithms & Data Structures

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| ALG-01 | Entities with pairwise relationships in the problem statement | **Graph in disguise** — most messy applied problems reduce to a classical graph problem once vertices/edges are designed | Can I name vertices/edges such that a catalog problem states my requirement? | S·p | algorithm-design-manual ch-8 |
| ALG-02 | Exhaustive enumeration proposed or coded | **Brute-force budget** — n! dies at 20 items, 2ⁿ past 40, n² past 10⁶; below those, brute force is the correct, simplest solution | What is realistic n, and which complexity row does the plan sit in? | S·p | algorithm-design-manual ch-2 |
| ALG-03 | Nested-loop search for duplicates, closest pair, mode, or rank | **When in doubt, sort** — sort-then-sweep (or a hash table) replaces the O(n²) loop | Would sorting first turn this into a linear scan? | S·w | algorithm-design-manual ch-4 |
| ALG-04 | Repeated linear scans for membership/min/max inside a loop | **Dictionary/heap reflex** — repeated scan-for-min is a heap; repeated scan-for-key is a hash table | Which operations repeat, and which structure serves exactly that set? | S·w | algorithm-design-manual ch-3 |
| ALG-05 | Optimization problem resembling longest path, TSP, coloring, cover, or partition | **Hardness recognition** — one word separates easy from NP-hard twins; check the catalog before promising exact-and-fast; then choose an escape deliberately (pruned exact / approximation / heuristic) | Is this a known NP-hard entry, and is my instance a polynomial special case? | S·p | algorithm-design-manual ch-11 |
| ALG-06 | Hand-rolled sort, RNG, date math, crypto, or geometry predicates in a diff | **Catalog before code** — these are solved problems with tuned, correct libraries; hand-rolling trades correctness for nothing | Which library implements this catalog problem? | S·r | algorithm-design-manual ch-14 |

## Naming & Comprehension

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| NAME-01 | Naming a new identifier | **Three-step name mold** — intent concepts, then codebase-lexicon words, then the codebase's existing word-order; full dictionary words, 2–4 per name | Does this name use the same concepts, words, and order as its siblings? | S·w | programmers-brain ch-8 |
| NAME-02 | Diff introduces a synonym for an existing domain term | **One word per concept** — synonyms make readers hunt for a nuance that doesn't exist (see [DOM-03]) | Does the project lexicon already have a word for this? | S·r | programmers-brain ch-8 |
| NAME-03 | Name implies a type/behavior the code doesn't have (`is*` non-Boolean, getter with side effects) | **No lying names** — linguistic antipatterns measurably raise cognitive load and seed persistent misconceptions | Do the name's implied type and side-effects match the body? | S·r | programmers-brain ch-9 |
| NAME-04 | Your preferred style/mold conflicts with the codebase's | **Consistency beats local quality** — consistent-and-mediocre outperforms good-but-inconsistent | Am I matching the convention or optimizing my taste? | S·w | programmers-brain ch-8 |
| NAME-05 | Diff adds a near-duplicate of an existing function with a similar name | **Clones get mischunked** — readers chunk the copy as the original and discard exactly the small difference; unify or make the difference loud | Will a reader who knows the original notice what differs? | S·r | programmers-brain ch-9 |
| NAME-06 | Cluster of vague/broken names in one region of a diff | **Bad names mark bug hotspots** — naming-violation sites statistically co-locate with defects; review those regions deeper | Where names are worst, has the logic had extra scrutiny? | J·r | programmers-brain ch-8 |

## UI & Visual Design

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| UI-01 | New CSS declares a raw literal for spacing, font-size, radius, shadow, or color where the project has tokens/scales | **Predefined scales only** — one-off values accumulate into inconsistency and re-litigate the same decision every time | Is this value from the project scale/tokens? | S·w | refactoring-ui ch-1 |
| UI-02 | State, trend, or series distinguished by hue alone; or text contrast below 4.5:1 / 3:1 | **Never color alone, never below WCAG** — pair color with an icon/label; flip to dark-on-tint when white-on-color fails contrast | Does every color signal have a second channel; does all text meet its floor? | B·r | refactoring-ui ch-5 |
| UI-03 | Margin within a group equals or exceeds margin between groups | **Unambiguous spacing** — spacing signals grouping; space around a group must exceed space within it | Is every intra-group gap smaller than the surrounding inter-group gap? | S·r | refactoring-ui ch-3 |
| UI-04 | Text hierarchy varies only `font-size` across roles | **Size isn't everything** — weight and a 2–3-step grey ramp communicate importance better than size alone | Could this hierarchy use weight or color instead of another size? | S·r | refactoring-ui ch-2 |
| UI-05 | Grey text, or white/black at reduced opacity, over a colored/image background | **Same-hue de-emphasis** — grey/alpha on color looks disabled and bleeds; hand-pick a same-hue shade | Is de-emphasized text sitting on non-white via grey or alpha? | S·r | refactoring-ui ch-2 |
| UI-06 | Destructive action styled as the big red primary button on a normal page | **Hierarchy over semantics** — destructive ≠ prominent; red-primary belongs on the confirmation step | Is delete competing with the page's true primary action? | S·r | refactoring-ui ch-2 |
| UI-07 | Shade generated at use-site via lighten()/darken() or a new near-duplicate hex | **Define shades up front** — a fixed scale per color; on-the-fly derivation breeds near-identical variants | Does this shade exist in the palette, or is it invented inline? | S·w | refactoring-ui ch-5 |
| UI-08 | Box-shadow values invented per component | **Elevation system** — a fixed shadow scale mapped to z-meaning, chosen by layer, not taste | Which elevation level is this, and is the shadow from the shared scale? | S·w | refactoring-ui ch-6 |

## Release Readiness

> Added 2026-07-11. Source: Fields, *Product Deploy Agents* (CC BY 4.0 — Jason Fields, jasonpfields.com, @fasonista) — a 7-lens pre-release audit pipeline; distilled at `distilled/engineering/product-deploy-agents-fields.md`. These rules govern the phase between "code reviewed" and "in production" that no other source in this lexicon covers.

| ID | Trigger | Rule | Answers | T·P | Src |
| --- | --- | --- | --- | --- | --- |
| RLSE-01 | release audited by a single reviewer or one flat checklist | **Non-overlapping lenses** — distinct threat models (UX/arch/QA/product/compliance) catch disjoint failure classes; checklists miss the adversarial layer | Which lens produced zero findings — did it actually run? | S·p | product-deploy-agents-fields ch-1 |
| RLSE-02 | ship argued past an unresolved gate verdict ("probably fine") | **Gates are not suggestions** — proximity makes the shipping team the worst judge of shipping; CONDITIONAL is red, not yellow | Is any gate unresolved, and who besides us says so? | B·r | product-deploy-agents-fields ch-1 |
| RLSE-03 | audit or release review begins without stated invariants / "working correctly" criteria | **Invariants before audit** — explicit per-persona criteria convert judgment calls into pattern matches; violating a stated invariant is automatic P0 | What does "working correctly" mean for this user, in writing? | S·p | product-deploy-agents-fields ch-2 |
| RLSE-04 | UI diff ships a screen with states that merely render | **Undesigned state is a bug** — loading/empty/error/offline/first-time/edge-input each intentionally designed, incl. focus and announcements (↔ a11y A11Y-24) | Which of the seven states did nobody design? | S·r | product-deploy-agents-fields ch-3 |
| RLSE-05 | failure path leaves the user believing success | **Silent failure is the worst failure** — a crash is honest; silent data loss is a trust violation, P0 by definition (extends [AGT-10], [OBS-08] to user-facing data) | Can data appear saved here while not durable? | B·r | product-deploy-agents-fields ch-5 |
| RLSE-06 | test plan covers happy path + malicious inputs only | **The adversarial user is in a hurry** — double-tap, force-quit, background mid-save aren't attacks, they're Tuesday; enumerate action × adverse condition × timing per flow step | What happens at each step under kill/background/offline/token-expiry? | S·p | product-deploy-agents-fields ch-5 |
| RLSE-07 | rollout plan goes 0→100%, or phases lack stop criteria | **Phased rollout is instrumentation** — the first 10% is production with a smaller blast radius; stop criteria (crash floor, flow-completion baseline) named before launch | What number, watched for how long, pauses this rollout? | S·p | product-deploy-agents-fields ch-8 |
| RLSE-08 | release plan has no rollback section | **Rollback written before ship** — at 2am you execute, you don't design; must answer whether the old build can read the new build's data (↔ [DATA-03], backward direction) | If we revert at 50%, what happens to data the new build wrote? | B·p | product-deploy-agents-fields ch-8 |
| RLSE-09 | expert/legal/clinical sign-off requested with a jargon document | **Plain-English expert gate** — exact quoted strings, specific yes/no questions, five-minute read, explicit "not your problem" section; jargon sign-off shifts liability without informing | Could the expert answer without asking what a term means? | S·w | product-deploy-agents-fields ch-8 |
