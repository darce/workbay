# OCI self-hosted GitHub runner (optional, private repos only)

Status: prepared 2026-10-19, not provisioned. The collapsed single-job
hosted CI (`scripts/ci_local.sh`) is projected inside the GitHub Free
2,000 included private minutes/month, so this runner is only worth it if
push volume grows past that or sub-minute-floor latency matters.

## Economics first

- GitHub Free: 2,000 private minutes/month included; Linux overage
  ~$0.006/min. Public repos: standard runners free, unlimited.
- A dedicated PAYG A1 runner VM (2 OCPU/12 GB) costs roughly $25+/month —
  more than ~4,000 overage minutes. Do not provision it to save $3.
- It wins only on an already-paid/already-free VM with spare capacity.

## Rules

- Private repos only. Never attach a self-hosted runner to a public repo
  (fork PR code would run on the host) — assessment 2026-08-14, F5.
- Never co-locate on `prod backend host` (prod recognition/demo host). Use a
  dedicated VM, dedicated `runner` user, tailnet-only SSH.
- Prefer just-in-time (JIT) ephemeral runners for isolation; the classic
  persistent runner in `scripts/install_gh_runner.sh` is the simple path
  for a single-operator private repo with no fork PRs.

## Provision

1. Create a dedicated OCI A1 (ARM64) Ubuntu 24.04 VM; join the tailnet.
2. `gh api -X POST repos/$REPO_SLUG/actions/runners/registration-token --jq .token`
3. On the VM: `GH_RUNNER_TOKEN=<token> bash scripts/install_gh_runner.sh`
   (installs under `/opt/actions-runner`, systemd service, label `oci`).
4. Change `.github/workflows/test.yml`: `runs-on: [self-hosted, linux, oci]`.
   Python 3.12, Node, and build tools must exist on the VM (the hosted
   `setup-python` step does not apply); install them in cloud-init.
5. Verify with a `workflow_dispatch` run, then set the account Actions
   spending limit to $0 (Settings → Billing) so no job can silently fall
   back to billed hosted minutes.

## Spending guard (do regardless of runner)

Settings → Billing → Actions: set the spending limit to $0 and remove
“pay as you go” consent if present. With the limit at $0, exhausting the
2,000 included minutes blocks jobs instead of charging — an honest,
visible failure, recoverable by dispatching the OCI runner or waiting
for the monthly reset.
