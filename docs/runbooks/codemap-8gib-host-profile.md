# Codemap on an 8 GiB host

The repo-root `.cbmignore` excludes the briefs tree and Markdown records in
reviews, lanes, tasks, rescues, upstream requests, and assessments. This reduces
prose processed when discovery includes `docs`. The policy keeps plans,
runbooks, source, and test inputs eligible. It takes effect on the next index.
[Upstream v0.10.8 discovery rules](https://github.com/DeusData/codebase-memory-mcp/blob/v0.10.8/docs/cbmignore.md)

## Reindex after landing

1. Set the operator's codemap configuration to `auto_watch=false` and restart
   any sessions with active watchers. These settings and the reindex schedule
   are operator-owned.
2. After landing, wait for post-merge work to finish and coalesce pending
   changes into one reindex. Wait for normal memory pressure. On macOS, check
   `memory_pressure` for a normal reading, or use at least 25% free as the
   threshold if the reading only reports a free percentage.
3. Make the one-time `index_repository` call against the landed repository
   root. Use the canonical project name shown by `list_projects`.

   ```json
   {"repo_path": "<repo-root>", "name": "<project-name>", "mode": "moderate"}
   ```

4. Require a nonzero edge count from `index_status` and use `search_graph` to
   verify that `_index_argv` resolves in `codemap_runner.py`.

## Verify selective docs coverage

Moderate mode skips the entire `docs` directory. When the host has enough
headroom and docs coverage is required, use `"mode": "full"` in the same
`index_repository` call. Check that edges are nonzero and both `_index_argv`
and the plan heading “implementation note — Singleton codemap reindex,
handoff-coordinated” resolve using `search_graph`. For the heading, search
`Section` nodes with the name pattern `.*Singleton codemap reindex.*`.

Use `check_index_coverage` on `the corresponding implementation note` and look for
`excluded` with reason `cbmignore`. If the file is absent from the bounded
coverage report, the rule-specific result for that file is unverified.

Use `git grep` or the WorkBay handoff tool `find_related_prior_work` for
questions about excluded records.

To restore records, remove the relevant rule or the whole `.cbmignore` and
reindex in full mode when the host has sufficient headroom.
