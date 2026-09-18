#!/usr/bin/env bash
# release_mcp_package.sh — tag a new release of mcp-workbay-orchestrator on the
# darce/workbay repo and print the consumer install URL.
#
# Usage: scripts/release_mcp_package.sh <version>
#   e.g. scripts/release_mcp_package.sh 0.1.1
#
# The script:
#   1. Updates pyproject.toml version to <version>.
#   2. Updates the [tool.hoisted] install_url to point at the new tag.
#   3. Adds a CHANGELOG.md heading for [<version>].
#   4. Commits "chore: release v<version>" on the current branch.
#   5. Tags v<version> and pushes both the commit and the tag.
#   6. Prints the git+ssh install URL for consumers.
#
# Requires: git, python3.
set -euo pipefail

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
    echo "Usage: $0 <version>  (e.g. $0 0.1.1)" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYPROJECT="$REPO_ROOT/pyproject.toml"
CHANGELOG="$REPO_ROOT/CHANGELOG.md"
REPO_SLUG="darce/workbay"
INSTALL_URL="git+https://github.com/${REPO_SLUG}.git@mcp-workbay-orchestrator-v${VERSION}#subdirectory=packages/mcp-workbay-orchestrator"

echo "==> Releasing mcp-workbay-orchestrator v${VERSION}"

# 1. Update pyproject.toml
python3 -c "
import re, sys
path = sys.argv[1]
version = sys.argv[2]
text = open(path).read()
text = re.sub(r'(^version = )\"[0-9]+\.[0-9]+\.[0-9]+\"', r'\g<1>\"' + version + '\"', text, flags=re.MULTILINE, count=1)
text = re.sub(r'(install_url = \").*?\"', r'\g<1>' + sys.argv[3] + '\"', text)
open(path, 'w').write(text)
" "$PYPROJECT" "$VERSION" "$INSTALL_URL"
echo "    pyproject.toml -> version $VERSION"

# 2. Add CHANGELOG entry if not present
if ! grep -q "\[${VERSION}\]" "$CHANGELOG"; then
    DATE=$(date +%Y-%m-%d)
    python3 -c "
import re, sys
path, version, date = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path).read()
entry = '## [' + version + '] — ' + date + '\n\n### Changed\n\n- Release v' + version + '.\n\n'
# Insert before first versioned section or at end of [Unreleased]
text = re.sub(r'(\n## \[(?!Unreleased))', lambda m: '\n' + entry + m.group(1), text, count=1)
if '[' + version + ']' not in text:
    text += '\n' + entry
open(path, 'w').write(text)
" "$CHANGELOG" "$VERSION" "$DATE"
    echo "    CHANGELOG.md -> added [$VERSION] entry"
fi

# 3. Commit + tag + push
git -C "$REPO_ROOT" add pyproject.toml CHANGELOG.md
git -C "$REPO_ROOT" commit -m "chore: release v${VERSION}"
git -C "$REPO_ROOT" tag "mcp-workbay-orchestrator-v${VERSION}"
git -C "$REPO_ROOT" push origin HEAD
git -C "$REPO_ROOT" push origin "mcp-workbay-orchestrator-v${VERSION}"

echo ""
echo "Released: mcp-workbay-orchestrator-v${VERSION}"
echo "Consumer install URL:"
echo "  pip install \"${INSTALL_URL}\""
