# Evidence Artifacts

How to persist forensic manifests and native observations in the existing Handoff
artifact store. This page describes what the code does today. The lifecycle that
would enforce the retention and release rules in the last sections is **not implemented
yet**; it is tracked internally.

## Purpose and scope

- **Use it** when a stopped or quarantined actor must be preserved for forensics, when a failed
  lane's workspace will be reaped, or when a diagnostic bundle must outlive its workspace.
- **Do not use it** for large or binary evidence (archive it, see below), for secrets
  (the store does not redact; `packages/mcp-workbay-handoff/src/workbay_handoff_mcp/artifact_index.py`
  has no redaction step), or for live-process state (the store holds searchable text, not a
  snapshot of a running process).
- The store is a SQLite sidecar, `mcp-artifacts.db`, separate from the handoff database
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/config.py`).

## What to record, and how

The writer is the `artifacts` domain tool on the handoff server, with `artifact.operation` set
to `record`, `search`, `get` or `purge` (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/api.py`).
`record` calls `record_artifact` (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/core.py`),
which requires `source_kind`, `source_label` and non-empty `content`. It returns the usual
envelope (`ok`, `data`) with `mutation` set to entity `artifact_source`, operation `upsert`.

- **Pass `task_ref` explicitly.** When it is omitted, the handoff resolves the active task
  by its four-step rule (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/shared_primitives.py`)
  and raises on ambiguity.
- **`source_label` is part of the identity.** A source is keyed by
  `(task_ref, lane_id, source_kind, source_label)`. Recording the same key with different
  content **overwrites** the earlier chunks and metadata. Metadata is replaced, never merged: a
  correction restates the complete metadata object (read back with `get`, one key edited) and
  changes the content. Use a label that is unique per manifest, for example
  `<lane_id>:<run_id>:manifest`.
- **Identical content is a no-op.** `upsert_source` compares the content hash only. A re-record
  with the same content returns `was_updated: false` and keeps the **original** `content_type`,
  `summary` and `metadata`, even when those differ. A re-record with different content overwrites
  all three. The dedupe is covered by `packages/mcp-workbay-handoff/tests/test_artifact_tools.py`
  (`test_record_artifact_dedupes_on_reindex`).
- **`content_hash` is truncated.** The store keeps the first 32 hex characters of the sha256
  of the content (`_content_hash` in `artifact_index.py`). Use the full sha256 fields below
  for proof.
- **`metadata` must be JSON, and is serialised only on write.** It is typed `dict[str, Any]`
  on the op and stored with `json.dumps`. That call runs only when the content changes (the
  insert or update branch), so a value that is not JSON-serialisable raises `TypeError` from
  `upsert_source` in that case. A re-record with identical content never serialises `metadata`,
  so the same value succeeds there and is not stored. `record_artifact` catches only
  `RuntimeError`, so the `TypeError` propagates rather than coming back as an error envelope.
  An empty `{}` is stored as null.

Recommended `metadata` object. These keys are a **recommended convention**. The store does
not enforce a schema: any non-empty JSON object is accepted and comes back from `get` as it
was written.

```json
{
  "task_ref": "<task ref>",
  "lane_id": "<lane id>",
  "run_id": "<run id>",
  "process_start_identity": {"pid": 4242, "start_token": "linux:<boot_id>:<start_ticks>"},
  "namespace": "<cgroup | unit | container | host>",
  "native_unit": "<native unit name, for example a systemd unit>",
  "source_hashes": {"<input name>": "<full sha256 hex of that input>"},
  "archive_ref": "<locator of the archive, when there is one>",
  "archive_sha256": "<full sha256 hex of the archive, when there is one>"
}
```

`process_start_identity` pairs the boot id with start ticks; the pair identifies one process across
pid reuse. `_read_process_identity` in
`packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/lane_retire_sweep.py`
reads start ticks from field 22 of `/proc/<pid>/stat` and emits the `start_token`. A `start_token`
missing either part is not an identity, and its evidence is recorded as incomplete.

## Large and binary evidence

- `content` is a string and the store indexes it as text. Do not put binary data in it.
- Write the archive to durable storage on the host that owns the lane, outside any temporary
  sandbox or worktree that cleanup will remove. Compute its full sha256 after writing and
  before recording it.
- Record only the reference, the size and the sha256, in the manifest that summarises the
  archive. The size goes in its searchable `content` text; `archive_ref` and `archive_sha256` go
  in its `metadata`. There is no separate archive artifact, so the manifest is recorded after the
  archive is written and hashed.

```
archive_ref:    /var/lib/example-evidence/lane-a/run-7/unit-7f3a.tar.zst
archive_size:   412938240 bytes
archive_sha256: ceac32289c53858ed3ca90c5228dc5be9607cdac74f1271679f59ed91e82cd2d
```

The path and digest above are made up.

Indexing behaviour, as read from the source:

- The `record` path has **no size or line threshold**. `record_artifact` passes any non-empty
  content to `upsert_source`, which indexes it. Plaintext is split every 50 lines, so a
  single-line body is one chunk however large it is.
- The thresholds are `RuntimeConfig.artifact_index_min_bytes` and `RuntimeConfig.artifact_index_min_lines`
  (`packages/mcp-workbay-handoff/src/workbay_handoff_mcp/config.py`; defaults `min_bytes=4096`
  and `min_lines=80`), passed to `maybe_record_artifact`. It indexes when the content is at least
  `min_bytes` bytes **or** has at least `min_lines` newlines. `lane_exec.py` returns early under
  `min_bytes` bytes (`packages/mcp-workbay-orchestrator/src/workbay_orchestrator_mcp/orchestration/lane_exec.py`), so the line rule never applies on that path.
- Chunks are derived text, not byte-exact copies. Markdown splits on H1–H3 headings,
  plaintext splits every 50 lines, and JSON gives one chunk per top-level key or list item,
  re-serialised with indent 2 (`chunk_content` in `artifact_index.py`). Keep the sha256 of
  the original bytes for byte-exact proof.

## Retrieval and verification

- Read back with the same domain tool. `get` takes `source_id`, or `task_ref` plus
  `source_label`; it returns the parsed `metadata` and the `chunks`. `search` takes `queries`
  and runs FTS5 over the chunks (`porter unicode61` tokenizer). `detail` and `fields` project
  the response.
- Before any cleanup acts on the actor:
  1. `get` the manifest by the `source_id` that `record` returned. Confirm that its `task_ref`,
     `lane_id`, `source_kind` and `source_label` match the recorded key, and that
     `process_start_identity`, `source_hashes` and `archive_ref` are present. Fall back to
     `task_ref` plus `source_label` only with the same check, since that lookup takes `LIMIT 1`
     with no `ORDER BY`.
  2. Re-hash the file at `archive_ref` and compare it with `archive_sha256`. A mismatch or a
     missing file means the evidence is not verified; stop, and preserve the actor.
  3. Re-hash each input in `source_hashes` that still exists. Record a mismatch under its own
     `source_label`, for example `<lane_id>:<run_id>:hash-mismatch`. Reusing the manifest's
     `source_label` overwrites the manifest.
- `purge` deletes source rows and their chunks, scoped by `task_ref`, `lane_id`, `app_root`
  or `older_than_days` (`purge_artifacts` in `artifact_index.py`). It has no archive check and
  no hold. Do not purge evidence until verification has passed.

## Separating three concerns

Three values are distinct. Do not merge them.

- **Execution authority**: the right to run or resume an actor. An expired execution
  authority never permits resuming a stopped actor.
- **Evidence retention**: how long the manifest and archive are kept. A retention-review
  date is a review trigger, not a lease expiry.
- **Resource occupancy**: what the actor still holds against capacity. A quarantined actor
  keeps its real resource charge until a qualified native release proof exists.

Intended policy: a retention-review date must not become a global execution-lease failure
for unrelated work that fits the remaining capacity.

**Not implemented.** The lifecycle that enforces these rules (capture, reconcile, then reap,
crash-resumable) is not implemented yet and is tracked internally. The `artifacts` domain
enforces none of them. Until that lifecycle exists, the rules above are guidance for
operators, not guarantees the system gives.

## Unknown ownership and incomplete capture

- If ownership of an actor is unknown, preserve the actor. Record one bounded follow-up
  artifact under its own `source_label` that states what is unknown, what evidence would settle
  it, and who must look. Reusing the manifest's `source_label` overwrites the manifest.
- Never authorise a broad kill (every process in a namespace, or every process matching a
  name), and never report a resource as released without a native release proof.
- A manifest whose `archive_ref` is set but whose `archive_sha256` is missing, or whose re-hash
  fails, is recorded as incomplete. Do not fill the missing field by guessing. A manifest with no
  archive is not incomplete for that reason. To correct a manifest, re-record it with changed
  content (for example an appended `corrected: <reason>` line) and restate the complete metadata
  object: read it back with `get`, edit only the wrong key, and send the whole object. A
  metadata-only re-record is ignored, and changed content replaces the metadata, so a partial
  object drops `archive_ref`, `archive_sha256`, `process_start_identity` and `source_hashes`.
- A searchable text snapshot alone is not sufficient evidence to clean up. Cleanup needs the
  verified archive and the native proof.

## Worked example

Not executable. Placeholders stand in for real identifiers.

```
1. Capture (no cleanup yet)
   Read the actor's identity from the host: pid, boot id and start ticks, namespace, native unit.
   Hash each input the manifest summarises (full sha256).

2. Archive the large evidence and hash it
   Write the archive to durable host storage and sha256 it in full. Keep its path
   (archive_ref) and digest (archive_sha256). The archive is not a separate artifact.

3. Record the manifest (one record call, after step 2)
   artifacts(artifact={"operation": "record", "task_ref": "<task_ref>", "lane_id": "<lane_id>",
                       "source_kind": "forensic-manifest",
                       "source_label": "<lane_id>:<run_id>:manifest",
                       "content_type": "text/markdown",
                       "content": "<searchable summary: actor state, unit, failure, archive size>",
                       "metadata": {<the recommended keys>, "archive_ref": "<archive_ref>",
                                    "archive_sha256": "<archive_sha256>"}})

4. Verify
   artifacts(artifact={"operation": "get", "source_id": "<source_id returned by step 3>"})
   Confirm task_ref, lane_id, source_kind and source_label match the recorded key, and that the
   manifest carries archive_ref and archive_sha256. Re-hash the file at archive_ref and compare
   it with archive_sha256. Equal: verified.

5. Only then cleanup
   Cleanup may proceed only after step 4 passes. The enforcing cleanup lifecycle is not
   implemented yet, so until it exists this is an operator decision. A failed or missing
   verification means: preserve the actor and record a follow-up.
```
