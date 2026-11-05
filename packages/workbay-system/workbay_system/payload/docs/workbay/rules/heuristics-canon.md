# Heuristics canon — consumer contract

WorkBay does not vendor or project heuristics; this is the consumer pointer and
authored pin contract. The operator's `heuristics-canon-current` link (or
`WORKBAY_HEURISTICS_CANON_ROOT`), verified against its manifest, is the reviewed pin.
The block below is the shipped fallback for hosts without that live source.
The payload file is the sole authored fallback pin source; the orchestrator
`_assets` copy is derived and kept byte-identical by an integrity test.
The canon lives in the [public repository](https://github.com/darce/heuristics-canon).

```json heuristics-canon-pin
{
  "repository": "https://github.com/darce/heuristics-canon",
  "release_tag": "v0.26.2",
  "manifest": "meta/release-manifest.json",
  "manifest_schema": "heuristics-canon/release@5",
  "release_commit": "1883d309b43665ba73b97da1b4aa4d2d1ee43f48",
  "manifest_sha256": "46636aa71f2e2e0c413b6ccff1cae4959543026c6ee7e4abde2bc7f30645a260"
}
```

## Pin and verify

Read the reviewed pin from `~/Development/heuristics-canon-current`, or from
`WORKBAY_HEURISTICS_CANON_ROOT` when configured. Verify its manifest and file
digests; lane prompts name the manifest's version, SHA-256 prefix and schema.
Prompts warn when the live release and the shipped fallback disagree, or when a
configured source cannot be trusted or read and the pointer pin must be used.

For the shipped fallback, check out `release_tag`; confirm `git rev-parse <release_tag>^{commit}` equals
`release_commit`, then confirm the SHA-256 of `manifest` equals `manifest_sha256`.
Only then compare SHA-256 digests of all `lexicons/*.md` files and every file under
`reasoning/` against the manifest. A moved tag, rewritten manifest, or digest mismatch
means this is not that release; stop.

## Bump

Moving the operator's link to a verified release is the operator's bump; there is
no silent upgrade. Refresh the shipped fallback after verifying its digests.
A bump of the block changes all six values in one reviewed commit.
Rule IDs keep their meaning; withdrawing an ID or card is major, adding one is minor, and editing
content at an existing path is patch. Before a major bump, compare every cited repository ID with
the new manifest's ID set and withdrawn-to-successor map; citations are schema references
([DATA-03] **Schema evolution compat** (Designing Data-Intensive Applications, ch. 4)). Report a
newer upstream release and keep the reviewed pin until the operator bumps it. Keep one authored pin source
([REF-45] **One authored token source** (Design Systems for Developers (Michael Mangialardi),
ch. 4)); the version stays pinned and retrievable ([REF-46] **Pin the dependency version**
(Continuous Delivery, ch. 2)).

## How to apply

Treat rule IDs as the canon's public API: published IDs keep their meaning until a major release
withdraws them. Rules are evidence-backed defaults; task instructions, observed facts, and
documented exemptions take precedence. Retrieve only rows triggered by the changed artifact, never
the full rule set. Tiers set force: `B` blocks, `S` is a strong default with named exemptions, and
`J` needs context; escalate context-poor `J` rows instead of skipping them. Inspect each kept row's
tier and exemptions.

1. Route the changed artifact through the canon routing table.
2. Open only the rule families listed by that route.
3. Keep only triggered rows; report untraceable sources as corpus defects,
   not as authority.
4. For a kept row listed in `PRINCIPLES.md`, inspect siblings from unrelated
   domains as mechanism checks, not as votes.
5. Open each reasoning card whose `Rule IDs` section lists a kept ID once, in
   ascending slug order, and follow its Verification section.
6. Resolve conflicts by partitioning the surface, object, or sequence; if
   that does not resolve them, record a contextual judgment. Do not average
   rules; apply both when they govern different stages.
7. Cite each kept rule beside its decision as `[ID] **Rule name** (Source as printed)`; deep-link as
   `lexicons/<file>.md#<lower-case-id>`. Label bootstrap, agent-operations, and video-pipeline-
   practice sources as `practice label`; never cite unfired rules or card slugs alone.
8. Stop when every applicable `B` and `S` row is satisfied, exempted with
   evidence, or escalated.

## Network-less sandboxes

The dispatcher does not currently inline canon rows or cards into dispatch
briefs. A network-less lane that cannot read the pinned release needs its brief
to carry the fired rows and their cards, tagged with that release. Until this
is implemented, if neither the canon contents nor inline rows and cards are
reachable, report canon guidance as unavailable; do not guess or claim rows
were applied. Nothing is copied into this tree.

Floating-release guidance is withdrawn. The operator's link, verified against its
manifest, takes precedence over this block and over older skills and notes.
