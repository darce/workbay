# Deferred Review Tracks — Engineering Knowledge Reserve

> **Purpose.** Two bodies of distilled engineering knowledge that were intentionally **not** embedded into the general coding-discipline skills under `internal`, because they belong to **different, dedicated review processes** rather than to general implementation/scoping/diff/plan review. They are preserved here verbatim-in-essence so a future dedicated review skill (a design-system / visual review, and a data-systems / storage-engine review) can consume them without re-reading the source texts — and so the knowledge survives independent of the external `context-alt-text-monorepo/literature/` tree.
>
> **Status:** reserved, not active. These tracks are **not** wired into any skill, lens, or checklist today. When a dedicated review skill is created, lift the relevant section into that skill's rule doc (mirroring how `refactor` embeds the Fowler smell table).
>
> **Sources:** *Refactoring UI* (Wathan & Schoger) → Track 1; *Designing Data-Intensive Applications* (Kleppmann) → Track 2.

---

## Track 1 — Design-System / Visual Review (source: Refactoring UI)

> Reserved for a future dedicated review skill; intentionally NOT embedded into the general coding-discipline skills (see internal).

### Process & method (work in grayscale first; pre-define systems)
- Design the first real **feature**, not the shell (nav/sidebar/logo) — shell decisions need feature knowledge you don't yet have.
- Sketch on paper with a thick Sharpie early; it blocks premature detail obsession. Wireframes are disposable.
- **Design in grayscale first** — forces hierarchy through spacing, contrast, and size alone before color is a crutch.
- Work in short cycles: simple version → build → iterate on the working UI → next feature. Be a pessimist; ship the smallest version; don't imply features you can't build.
- **Never hand-pick from unlimited values.** Pre-define constrained scales up front for: font size, font weight, line height, color, margin, padding, width, height, box-shadow, border-radius, border-width, opacity.
- Decision method when choosing a value: guess the best value, try the two adjacent scale values, pick the obvious winner.

### Personality / brand tone
- **Serif** → elegant/classic; **rounded sans-serif** → playful; **neutral sans-serif** → plain/versatile.
- **Blue** → safe/familiar; **gold** → expensive/sophisticated; **pink** → fun/informal.
- **Small border-radius** → neutral; **large** → playful; **none** → serious/formal. Stay consistent — mixing square and rounded corners looks worse.
- Copy tone matters as much as visual choices.

### Hierarchy & emphasis
- Visual hierarchy is the single biggest factor in "looks designed." Deliberately de-emphasize secondary/tertiary content.
- Don't rely on size alone — use **weight and color** too.
  - Primary content: dark color. Secondary: medium grey. Tertiary: lighter grey.
  - Emphasized text: weight **600–700**. Normal text: weight **400–500**.
  - **Never use font weight below 400** for UI text — de-emphasize via lighter color or smaller size instead.
- **Don't use grey text on colored backgrounds** (grey de-emphasizes only against white). Don't use white + reduced opacity (looks disabled, bleeds through images). Hand-pick a color sharing the background's hue with adjusted saturation/lightness.
- **Emphasize by de-emphasizing**: if the primary element won't pop, soften the competitors (e.g. soften inactive nav items; remove a competing sidebar's background entirely).
- **Labels are a last resort** — format often signals type (email, phone, price); context often signals it ("Customer Support" under a name). Prefer "12 left in stock" over "In stock: 12". When required, treat labels as supporting content (smaller, lower-contrast, lighter weight). Exception: if the user scans *for* the label (tech specs), emphasize the label and keep data slightly lighter.
- **Separate visual from document hierarchy** — semantic tag (h1–h6) ≠ visual size. Section titles often act as labels (small); it's fine to visually hide them when content speaks for itself.
- **Balance weight and contrast** — bold feels emphasized because more pixels are covered. Lower the contrast of heavy icons sitting next to text. For too-subtle borders, increase **width** rather than darkening color.
- **Action hierarchy** (every action is in a pyramid): Primary → solid high-contrast background; Secondary → outline or low-contrast background; Tertiary → link style. A destructive action is not automatically big/red/bold — give it secondary/tertiary treatment on the page, reserving big/red/bold for the confirmation step where it is the primary action.

### Spacing & layout
- **Start with too much white space**, then remove until satisfied (the default instinct produces cramped UIs). Dense UIs are valid but must be a deliberate choice.
- **Spacing/sizing system**: no two adjacent scale values should differ by **less than ~25%**. Base value **16px** (browser default; divides cleanly). Practical px scale: **4, 8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256, 384, 512, 640, 768**. Small-end differences matter more (12→16 = 33% jump); large-end barely register.
- **You don't have to fill the screen** — if 600px is optimal, use 600px and leave edge padding. Use `max-width` on components; don't force full-width just because the nav is. Mobile-first: start at ~400px canvas. Narrow content in a wide UI → split into columns rather than stretch.
- **Grids are overrated** — fluid percentage widths are wrong for elements with an optimal fixed width. Give sidebars a fixed width, let main content flex. Use `max-width` for centered elements (login card) instead of grid percentages. Never shrink below optimal size just because the grid says so.
- **Relative sizing doesn't scale** — don't define headline as 2.5em of body. Desktop 18px body / 45px headline (2.5×); small screen 14px body / 20–24px headline (1.5–1.7×) — totally different ratio. Large elements shrink faster than small ones. Don't define button padding as em of font-size; larger buttons need disproportionately more padding, smaller ones less — scale independently.
- **Avoid ambiguous spacing** — spacing *between* groups must exceed spacing *within* groups. In forms: margin below an input > margin below its label (label sticks to its input). For article headings: more space *above* than below (heading belongs to what follows). Apply to lists and horizontal groups too.

### Typography scale & text
- **Type scale** — pre-define; avoid arbitrary px. Modular (ratio) scales produce fractional px and too few sizes. Hand-crafted px scale: **12, 14, 16, 18, 20, 24, 30, 36, 48, 60, 72**. Use `px` or `rem`, **never `em`** for scale definitions (em compounds off-scale in nested elements).
- **Fonts** — safe pick: neutral sans-serif / system stack (`-apple-system, Segoe UI, Roboto, Noto Sans, Ubuntu, Cantarell, Helvetica Neue`). Filter Google Fonts to ≥5 weights (10+ styles incl. italics) — cuts ~85%, leaves <50 sans-serifs. Avoid condensed faces with short x-height for body text.
- **Line length** — optimal **45–75 characters per line**; CSS `width: 20–35em`. Cap paragraph width even when the container is wider.
- **Baseline, not center** — when mixing font sizes on one line, align to the **baseline**, not vertical center.
- **Line-height is proportional & inverse to font size** — narrow/short content → ~1.5; wide content → up to ~2; small text → taller; large headlines → 1 is fine.
- **Links** — in body prose surrounded by non-links, make links clearly stand out (color + underline). In link-dense UI, use heavier weight or darker color instead. Ancillary links: underline/color on hover only.
- **Alignment** — most text left-aligned. Center only headlines / short blocks (≤2–3 lines). Right-align numbers in tables so decimals line up. Justified text needs `hyphens: auto` to avoid gaps.
- **Letter-spacing** — default: leave it alone. Decrease on wide-spaced UI fonts used as headlines; **increase on all-caps**. Don't use tight-spaced headline fonts at small sizes.

### Color / HSL & palettes
- **Use HSL, not hex** — Hue 0–360° (0 red, 120 green, 240 blue), Saturation 0% grey → 100% vivid, Lightness 0% black / 50% pure / 100% white. HSL ≠ HSB; browsers understand HSL.
- **You need more colors than you think** — three categories: **greys (8–10 shades)**, **primary (5–10 shades)**, **accents (multiple colors, 5–10 shades each)**. Avoid true black; start from very dark grey. Accents: eye-grabbing (yellow/pink/teal) for highlights; semantic (red destructive, yellow warning, green positive). Complex UIs: ~10 colors × 5–10 shades.
- **Define shades up front** — never call `lighten()`/`darken()` on the fly. Build a fixed **9-shade scale (100–900, base 500)**. Process: pick base → pick lightest (tinted backgrounds) and darkest (text) → fill 700 & 300 → fill 800, 600, 400, 200. Trust your eyes over math; resist new off-system shades.
- **Don't let lightness kill saturation** — as lightness nears 0%/100%, colors wash out; **increase saturation as lightness moves away from 50%**. If base saturation is already 100%, rotate hue instead.
- **Perceived brightness by hue** — brightness = √(0.299R² + 0.587G² + 0.114B²). Bright hues (maxima): **yellow 60°, cyan 180°, magenta 300°**. Dark hues (minima): **red 0°, green 120°, blue 240°**. To lighten without losing intensity, rotate toward the nearest bright hue; to darken, toward the nearest dark hue. Limit rotation to **≤20–30°** or it reads as a different color; combine with lightness adjustment.
- **Greys don't have to be grey** — practical greys are saturated. Cool greys: saturate with blue; warm greys: with yellow/orange. Increase saturation for the lightest and darkest grey shades to hold temperature.
- **Accessible ≠ ugly** — WCAG: normal text (<~18px) needs **4.5:1**; large text **3:1**. Dark-bg + white text often forces very dark backgrounds — **flip the contrast**: dark text on a light colored background. For colored-on-colored, shift hue toward a brighter hue (cyan/magenta/yellow) to raise contrast without going to pure white.
- **Don't rely on color alone** — color-blind users can't read red/green trends; add an icon, label, or pattern. In graphs, differentiate by light-vs-dark contrast rather than hue.

### Depth / elevation & shadows
- **Emulate a single light source from above** — top edges lighter, bottom edges darker. Raised: light top inset-shadow + small dark drop-shadow below. Inset: light bottom inset-shadow + small dark inset-shadow at top. Hand-pick the lighter color (don't use semi-transparent white — washes saturation).
- **Shadows convey elevation** — small/tight shadow = slightly raised (buttons); medium = dropdowns/floating; large = modals/dialogs. Define a **5-shadow elevation system** (smallest → largest, linear). Interactive: on drag → larger shadow; on press → smaller/none. Pick shadows by elevation, not aesthetics.
- **Two-part shadows** — Shadow 1 (large/soft, bigger offset + blur) = direct-light shadow; Shadow 2 (tight/dark, small offset + blur) = ambient occlusion. At high elevation make the tight dark shadow subtler; at low elevation it's most distinct.
- **Flat designs can have depth** — lighter than background = raised, darker = inset. Flat alternative: a short vertically-offset shadow with **zero blur radius**.
- **Overlap to create layers** — offset cards across a background transition; make elements taller than their parent to overlap both sides. For overlapping images, add an "invisible border" matching the background color.

### Images & backgrounds
- **Use good photos** — bad photos ruin good designs. Hire a photographer or use high-quality stock. Never design with placeholders intending to swap in phone shots later.
- **Text needs consistent contrast over images** (combine techniques): semi-transparent overlay (black helps light text, white helps dark text); lower image contrast + adjust brightness; colorize (desaturate + lower contrast + solid fill in multiply blend); text shadow (large blur, no offset — subtle glow).
- **Everything has an intended size** — scaling bitmaps up = fuzzy; SVGs look chunky at 3–4×. For large icons, enclose a near-intended-size icon in a shape with background color. Don't scale screenshots down 70% — use a smaller-screen screenshot, a partial crop, or simplified line-art. To shrink icons, redraw a simplified version at target size.
- **User-uploaded content** — center inside fixed containers and crop with `background-size: cover`. If an image blends into the UI background, use a subtle **inner box-shadow** (not a border), or a semi-transparent inner border.

### Finishing touches, empty states & components
- **Supercharge defaults** — replace bullets with icons; promote blockquote marks to large colored elements; custom thick/colorful link underlines; brand-colored custom checkbox/radio states.
- **Accent borders** add flair without graphic skill: top of a card, left edge of alerts, under active nav items, short accent under headlines, across the top of the whole layout.
- **Decorate backgrounds** — shift section background color, optionally a slight gradient (**hues ≤30° apart**); add a subtle repeating pattern at low contrast; or a corner geometric shape / partial pattern at low contrast.
- **Don't overlook empty states** — they are the new user's first impression. Include an illustration + prominent CTA; hide irrelevant tabs/filters until content exists.
- **Use fewer borders** — prefer box-shadow, different adjacent background colors, or extra spacing. If using different backgrounds *and* a border, try removing the border first.
- **Think outside the box** — dropdowns can have multiple columns/sections/icons; tables can combine columns, add images, use color; radio buttons can become selectable rich cards. Ask what a component must *do*, not what it conventionally looks like.

### Leveling up (review-practice meta)
- Study admired designs specifically for decisions you *wouldn't* have made yourself.
- Rebuild favorite interfaces from scratch without inspecting dev tools — forces you to discover the micro-decisions that produce polish.

---

## Track 2 — Data-Systems / Storage-Internals Review (source: Designing Data-Intensive Applications)

> Reserved for a future dedicated review skill; intentionally NOT embedded into the general coding-discipline skills (see internal).

*Each item: one-line essence + why it's deferred (it matters only when **building or operating** a datastore/engine, not when an application **calls** one through its public API).*

### Foundations & performance-internals (Ch. 1)
- **Fault vs. failure** — a fault is one component off-spec; failure is the whole system not serving. *Deferred: redundancy/blast-radius design is a platform concern; app code just retries.*
- **Percentile / tail-latency mechanics** — measure with p50/p95/p99/p999, never the mean; **never average percentiles — add histograms** (forward decay, t-digest, HdrHistogram). Tail-latency amplification: N parallel backend calls → user sees the max. Head-of-line blocking: a few slow requests stall fast ones. *Deferred: matters when instrumenting/operating a service tier, not when issuing one query.*
- **Load-parameter modeling & fan-out tradeoffs** (Twitter read-time-join vs. write-time-mailbox vs. hybrid). *Deferred: capacity/architecture work, re-evaluated per ~10× load growth.*

### Storage engine internals (Ch. 3)
- **Log-structured hash index (Bitcask)** — append-only log + in-memory hash (key→offset); all keys must fit RAM; one seek per read; no range queries. *Deferred: only when implementing/tuning the engine.*
- **Compaction & tombstones** — background merge of segments discards superseded keys; deletes are tombstone records. *Deferred: engine-internal GC; callers never see it (operators must monitor it).*
- **SSTables + LSM-trees** — sorted segments enable merge-sort compaction over files larger than RAM, sparse in-memory index, block compression. Write path: memtable → flush to SSTable → compact; read path: memtable → newest → older SSTables; **WAL** restores the memtable after crash; **Bloom filters** skip disk reads for absent keys. Strategies: **size-tiered** (HBase) vs. **leveled** (LevelDB/RocksDB). *Deferred: internals of LSM engines (Cassandra, RocksDB, Lucene).*
- **B-trees** — fixed-size pages (~4 KB), branching factor ~several hundred (4-level × 500 fan-out ≈ **256 TB**), in-place page overwrite + splits, **WAL** first, **latches** for concurrency; optimizations: copy-on-write (LMDB), key abbreviation, sibling leaf pointers, fractal trees. *Deferred: relational-engine internals.*
- **B-tree vs. LSM tradeoffs** — B-trees: faster reads, slower writes, predictable latency, key in exactly one place (easy range locks). LSM: faster writes, better compression, slower reads, compaction-induced tail latency; **compaction that can't keep up → disk fills, reads slow (engines don't auto-throttle)**. *Deferred: engine-selection / operation decision.*
- **Write / read / space amplification** — one logical write → many physical writes; SSD wear makes minimizing it matter. *Deferred: storage-layout/hardware-longevity concern.*
- **Index structures** — secondary indexes, **heap file** + forwarding pointers, **clustered index** (InnoDB PK), **covering index**, **concatenated index** (useless on second field alone), **multi-dimensional / R-tree / space-filling-curve geospatial**, fuzzy/full-text (FSA + Levenshtein automaton). *Deferred: schema-physical-design / index-implementation.*
- **In-memory DB internals** — speed comes from avoiding **encoding-to-disk-friendly-structure** overhead, not from avoiding disk reads; durability via battery-backed RAM / WAL / snapshots / replication; anti-caching evicts at record granularity. *Deferred: engine implementation detail.*
- **OLTP vs. OLAP, star/snowflake, column-oriented storage** — column stores group a column's values; **bitmap + run-length encoding** enables bitwise multi-predicate filtering; **vectorized processing** (L1-cache chunks, SIMD); sort-order chosen for compression; data cubes/materialized views as precomputed aggregates. *Deferred: warehouse/analytics-engine internals.*

### Encoding / serialization formats (Ch. 4)
- **Backward vs. forward compatibility** — new code reads old data (backward) vs. old code reads new data (forward, harder — must ignore unknown fields). *Deferred: matters when designing a persisted/wire schema, not when consuming a typed object.* (Note: the *application-level* compat rule IS embedded — see `engineering-heuristics.md` §Data & Consistency. This entry is the wire-format mechanics behind it.)
- **Avoid language-specific serialization** (Java serialization) — lock-in + RCE-on-deserialize risk. *Deferred: format-choice gate.*
- **JSON/XML/CSV pitfalls** — int/float/string ambiguity, >2^53 precision loss, Base64 +33% for binary. *Deferred: interchange-format design.*
- **Thrift / Protobuf wire mechanics** — schema-required, **field tag numbers** not names; CompactProtocol varints. Evolution rules: add optional fields with new tags; never remove required, never reuse/change tags; rename freely. *Deferred: authoring/migrating the schema itself.*
- **Avro reader/writer schema resolution** — no tag numbers; decode by **matching field names** between writer and reader schemas; writer's schema stored per-file / version-number + **schema registry** / negotiated on connection; enables **dynamically generated schemas**. *Deferred: schema-registry / data-pipeline engineering.*
- **Dataflow modes** — through DB (needs *both* compat directions; "data outlives code"), through services (REST vs RPC; **RPC failure-mode flaws** — timeout ≠ failure, retries duplicate unless idempotent), through async brokers, distributed actors. *Deferred: cross-service rolling-upgrade design.*

### Replication mechanics (Ch. 5)
- **Single-leader** — writes to leader, followers apply the replication log; **sync / async / semi-sync**; new-follower setup snapshot → copy → catch up (PG LSN / MySQL binlog). *Deferred: you operate this; app just connects.*
- **Failover internals** — detect → elect → reroute; failure modes: async write loss, **split brain** (STONITH), auto-increment/Redis divergence, timeout tuning. *Deferred: cluster-operation.*
- **Replication-log types** — statement-based (breaks on `NOW()`/`RAND()`), **WAL shipping** (format-coupled), **logical/row-based** (enables CDC), trigger-based. *Deferred: engine/CDC implementation.*
- **Replication-lag guarantees** — **read-your-writes**, **monotonic reads**, **consistent prefix reads**. *Deferred: client-routing layer.* (Note: the *anomaly recognition* IS embedded — see `engineering-heuristics.md` §Data & Consistency. This is the implementation of the fix.)
- **Multi-leader** — multi-DC / offline clients / collaborative editing; topologies (all-to-all vs star/circular, loop prevention via node IDs). *Deferred: topology design.*
- **Conflict resolution** — avoidance (route record to same leader), **LWW** (lossy), **CRDTs**, on-write/on-read handlers (Riak siblings), **version vectors** `{node:seq}`. *Deferred: building multi-master conflict handling.*
- **Leaderless / Dynamo-style** — parallel writes/reads, **read repair**, **anti-entropy**. **Quorum math: w + r > n** (typical n=3,w=2,r=2); **sloppy quorums + hinted handoff** raise availability but break the guarantee. *Deferred: leaderless-engine internals.*

### Partitioning / rebalancing internals (Ch. 6)
- **Key-range vs hash partitioning** — range enables range queries but hot-spots on prefixes; hash spreads load but kills range queries; Cassandra compound key. *Deferred: partition-key/schema design.* (Note: the *hot-spot red flag* IS embedded.)
- **Secondary-index partitioning** — **local/document-partitioned** (cheap writes, scatter-gather reads) vs **global/term-partitioned** (cheap reads, multi-partition writes). *Deferred: index-physical-design.* (Note: the *tradeoff decision* IS embedded.)
- **Rebalancing strategies** — **fixed partition count**, **dynamic split/merge** (HBase ~10 GB), **proportional to nodes** (Cassandra ~256 vnodes); automated rebalancing can cascade → require manual confirmation. *Deferred: cluster-operation.*
- **Request routing / service discovery** — node-forwarding vs routing tier vs partition-aware client; **ZooKeeper** vs **gossip**. *Deferred: datastore-internal coordination.*

### Transaction-isolation internals (Ch. 7)
- **ACID precision** — Atomicity = all-or-nothing (not concurrency); **Consistency is the application's job**; Isolation = as-if-serial; Durability = WAL+fsync / replication. *Deferred: implementing/selecting an isolation level.*
- **Read committed** — no dirty reads/writes; does NOT prevent read skew, lost update, write skew. *Deferred: isolation-level selection.*
- **Snapshot isolation / MVCC mechanics** — consistent snapshot at txn-start; readers don't block writers; per-row `created_by`/`deleted_by` txids; naming chaos. Prevents dirty reads & read skew, NOT lost update / write skew / phantoms. *Deferred: engine concurrency implementation.*
- **Race-condition catalogue** — dirty read/write, **read skew**, **lost update** (+ five fixes: atomic ops, `SELECT FOR UPDATE`, detect-and-retry, compare-and-set, app-merge), **write skew** (on-call-doctors case), **phantoms**. *Deferred: reasoning about a chosen isolation level's anomalies.* (Note: lost-update / write-skew *recognition* IS embedded as a review red flag; the isolation-level mechanics here are the deferred depth.)
- **Serializability mechanisms** — **actual serial execution** (single core, stored procedures); **2PL** (shared/exclusive locks, **predicate / index-range locks** for phantoms, deadlock detection); **SSI** (optimistic MVCC + serialization-hazard tracking; PostgreSQL ≥9.1, CockroachDB). *Deferred: concurrency-control-engine internals.*

### Distributed-systems theory internals (Ch. 8–9)
- **Partial failure & unreliable networks** — async networks give unbounded delay; only signal is "no response within timeout"; failures are indistinguishable. *Deferred: protocol-design assumption.*
- **Unreliable clocks** — time-of-day vs monotonic; NTP ~35 ms internet; **timestamps for ordering lose data under skew**; **Spanner TrueTime** `[earliest, latest]` + commit-wait; **Lamport logical clocks**. *Deferred: inside a coordination protocol.*
- **Process pauses & fencing tokens** — GC/VM-suspend can make a live node look dead; a paused leader returning is dangerous → **fencing tokens** (monotonic; storage rejects tokens ≤ last seen; ZooKeeper `zxid`). *Deferred: lock-service implementation.* (Note: the *fencing-token red flag* IS embedded; this is the mechanism.)
- **Byzantine faults & system models** — arbitrary/malicious nodes (needs 2/3 honest); timing models (sync / **partially synchronous** / async); failure models (crash-stop / **crash-recovery** / Byzantine); **safety vs liveness**. *Deferred: algorithm-correctness theory.*
- **Linearizability** — single-copy illusion / recency on individual reads/writes; **vs serializability** (multi-object isolation; SSI is *not* linearizable). Required for locks/leader-election, hard uniqueness, cross-channel timing; single-leader/consensus can be linearizable, multi-leader/leaderless generally not. *Deferred: a guarantee a datastore provides.* (Note: "linearizability → consensus" *decision* IS embedded; the proof is deferred.)
- **CAP nuance** — not "pick 2 of 3"; **Consistent OR Available when Partitioned**; narrow scope; mostly historical. Attiya-Welch: linearizable-op latency ≥ network-delay uncertainty. *Deferred: design-tradeoff framing for store builders.*
- **Causality & ordering** — partial order; **causal consistency = strongest model available under partitions without network-delay cost**; **Lamport timestamps** `(counter, node)` give total order but can't distinguish concurrent vs causal. *Deferred: ordering-protocol internals.*
- **Total order broadcast** — reliable + totally-ordered delivery ≡ state-machine replication ≡ **linearizable CAS register ≡ consensus** (all the same problem). *Deferred: replication/consensus implementation.*
- **Consensus & 2PC** — formal properties; **FLP impossibility**; **2PC** flow (prepare → unanimous → commit point → retry forever), coordinator-failure blocking with in-doubt locks, **XA** (~10× slower, SPOF coordinator); **fault-tolerant consensus** (Paxos/Raft/Zab/VSR): epoch/term numbers, unique leader per epoch, overlapping quorums, strict majority. *Deferred: core of building a coordination/consensus service — never reimplemented by app code.* (Note: "2PC cost" *heuristic* IS embedded; the protocol internals are deferred.)
- **ZooKeeper/etcd internals** — small in-memory data replicated via fault-tolerant total-order broadcast; linearizable CAS (leases), `zxid` fencing, ephemeral-node failure detection, watches; 3–5 nodes; coordination data, not runtime state. *Deferred: you call it as an operator; you don't build it.*

### Derived-data & dataflow internals (Ch. 10–12)
- **Batch internals** — MapReduce, **join algorithms** (reduce-side sort-merge, broadcast/partitioned hash join, map-side merge), skew/hot-key handling, immutable-input fault tolerance, Spark RDD lineage, Flink checkpointing, Pregel/BSP. *Deferred: data-platform/pipeline-engine engineering.*
- **Stream internals** — partitioned logs (Kafka offsets, replay), **CDC** (parse the replication log; Debezium/Maxwell), **event sourcing** ("the log is the truth, the DB is a cache"; CQRS), event-time vs processing-time + watermarks, window types, stream joins, **exactly-once / effectively-once** via idempotence + offset dedup + fencing. *Deferred: stream-processing-framework internals.*
- **Data-integration & correctness theory** — single-source-of-truth + derive others (avoid dual writes), log-based derivation, lambda → unified batch+stream, unbundling databases, **end-to-end argument** (correctness needs app-endpoint knowledge → **end-to-end request UUID with a UNIQUE constraint**), uniqueness-requires-consensus, **timeliness vs integrity** (integrity ≫ timeliness), compensating transactions, auditability (Merkle trees). *Deferred: whole-organization data-architecture review, not per-PR app review.*
