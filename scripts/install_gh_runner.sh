#!/usr/bin/env bash
# install_gh_runner.sh — register THIS host as an ephemeral-capable GitHub
# self-hosted runner for $REPO_SLUG (private repo only).
# Run ON a dedicated OCI VM (never the prod backend host prod host), as root.
# Usage: GH_RUNNER_TOKEN=<registration-token> bash install_gh_runner.sh
# Get a token (expires in 1h): gh api -X POST repos/$REPO_SLUG/actions/runners/registration-token --jq .token
# Self-hosted minutes are unbilled. NEVER register a runner for a public repo:
# fork PRs would execute untrusted code on the host.
set -euo pipefail
REPO_SLUG="${REPO_SLUG:?set REPO_SLUG=<owner>/<private-repo>}"
REPO_URL="https://github.com/$REPO_SLUG"
VERSION="2.321.0"
ARCH="$(uname -m)"; [ "$ARCH" = "aarch64" ] && ARCH="arm64" || ARCH="x64"
id runner >/dev/null 2>&1 || useradd -m -s /bin/bash runner
mkdir -p /opt/actions-runner && cd /opt/actions-runner
curl -fsSL -o runner.tar.gz "https://github.com/actions/runner/releases/download/v${VERSION}/actions-runner-linux-${ARCH}-${VERSION}.tar.gz"
tar xzf runner.tar.gz && rm runner.tar.gz
chown -R runner:runner /opt/actions-runner
sudo -u runner ./config.sh --url "$REPO_URL" --token "${GH_RUNNER_TOKEN:?set GH_RUNNER_TOKEN}" \
  --labels "oci,ephemeral-ci" --name "oci-$(hostname)" --unattended --replace
./svc.sh install runner && ./svc.sh start
echo "Runner installed. Now set runs-on: [self-hosted, linux, oci] in .github/workflows/test.yml"
