# Runbook — grok offload egress-deny (network-enforced trace-upload block)

> Second trusted control for grok offload, paired with the load-bearing
> shallow-clone sandbox (`orchestration/secure_sandbox.py`, task
> internal).

## Threat recap

grok Build (>=0.2.93) bundles a session `tar.gz` (the workspace + its git object
DB) and uploads it, ignoring the (architecturally bypassed) opt-out. Wire-level
and binary evidence (`strings ~/.grok/bin/grok`):

- The trace archive is built by `xai-grok-pager/src/trace_cmd.rs` (`session tar.gz
  archive`, `trace_config.json`, `export_metadata.json`) and uploaded via **GCS
  multipart** (`S3PutObject` / `CreateMultipartUpload` / `X-Goog-Api-Client`) to
  the bucket **`grok-code-session-traces`**.
- Pre-signed upload URLs are minted through the API host **`api.x.ai/v1`**
  (config keys: `trace_upload_endpoint_url`, `custom_upload_url`,
  `bucket_url_source`, `direct_upload_configured`).
- Codebase upload is skipped when the session reports `data_collection_disabled`
  / `no_storage_config` (`xai-grok-workspace/src/handle.rs`).

## Why a blanket host block does NOT work

`api.x.ai` carries BOTH inference (`https://api.x.ai/v1`, required for grok to
function) and the pre-signed-URL minting for uploads. Blocking it breaks grok.
So egress-deny must target the **GCS trace bucket**, not the API host.

## Load-bearing control comes first

The shallow clone (`git clone --no-local --depth=1`) already bounds what grok can
bundle to the single HEAD tree — verified: `rev-list --all` == 1 commit, no
history, no hardlinked object store. **This runbook is defense-in-depth**, for
non-ZDR consumers and against future policy changes. On a ZDR-team account
uploads are already gated server-side (`upload_reason:"zdr_team"`,
`uploads_enabled:false` in `~/.grok/logs/unified.jsonl`).

## Control 1 — force ZDR mode + config opt-out (cheap, in-sandbox)

The sandbox already writes the verified config opt-out
(`[features] telemetry=false`, `[telemetry] trace_upload=false`) and exports
`GROK_ZDR_ENABLED=1` into grok's process env (a real grok env var). These are
honored-if-cooperative; never trusted alone.

## Control 2 — network egress-deny of the GCS trace bucket

The bucket host depends on the pre-signed-URL addressing style. Capture the
actual CONNECT target once with the validation proxy (below), then block it.

### macOS (pf) — virtual-hosted bucket subdomain

```sh
# /etc/pf.anchors/grok-egress-deny
# Blocks the GCS trace bucket without touching api.x.ai inference.
block drop out proto tcp to grok-code-session-traces.storage.googleapis.com
```
Load with: `sudo pfctl -a grok-egress-deny -f /etc/pf.anchors/grok-egress-deny`.

### Linux/OCI (iptables) — resolve + drop

```sh
for ip in $(dig +short grok-code-session-traces.storage.googleapis.com); do
  sudo iptables -A OUTPUT -d "$ip" -j DROP
done
```

### /etc/hosts blackhole (portable, coarse)

```
0.0.0.0 grok-code-session-traces.storage.googleapis.com
```

> CAVEAT — **path-style** pre-signed URLs use
> `storage.googleapis.com/grok-code-session-traces/…`; that shares the host with
> all of GCS and CANNOT be host-blocked without collateral. Path-blocking needs
> an HTTPS forward proxy (see validation). This is why the shallow clone, not the
> egress-deny, is the load-bearing control.

## Validation — CONNECT-logging proxy (no MITM required)

To see WHICH hosts grok contacts (hostnames only, payloads stay encrypted), run
grok inside the sandbox behind a CONNECT-logging forward proxy and inspect the
`CONNECT <host>:443` lines. A proxy that 403s the trace bucket while allowing
`api.x.ai` proves grok still functions with uploads denied (uploads are
non-blocking by design — see `block_for_upload` / `upload_flush_timeout_secs`).

On a ZDR-team host the proxy log will show ZERO trace-bucket CONNECTs regardless
(uploads gated server-side), so a positive "blocked an upload" signal requires a
non-ZDR test account. The structural clone guarantee does not depend on this
capture.
