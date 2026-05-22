#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/knyteowl/flow-intelligence-poc-template.git}"
DEST="${DEST:-/opt/flow-intelligence-poc-src}"
BRANCH="${BRANCH:-main}"

fail() { echo "ERROR: $*" >&2; exit 1; }
[ "$(id -u)" -eq 0 ] || fail "run as root, e.g. curl ... | sudo bash"
command -v git >/dev/null 2>&1 || { apt-get update && apt-get install -y git; }

if [ -n "${GITHUB_TOKEN:-}" ] && [[ "$REPO_URL" == https://github.com/* ]]; then
  AUTH_REPO_URL="${REPO_URL/https:\/\/github.com/https:\/\/x-access-token:${GITHUB_TOKEN}@github.com}"
else
  AUTH_REPO_URL="$REPO_URL"
fi

if [ -d "$DEST/.git" ]; then
  git -C "$DEST" fetch origin "$BRANCH"
  git -C "$DEST" checkout "$BRANCH"
  git -C "$DEST" pull --ff-only origin "$BRANCH"
else
  rm -rf "$DEST"
  git clone --branch "$BRANCH" "$AUTH_REPO_URL" "$DEST"
fi
# Remove token from persisted remote URL if one was used.
git -C "$DEST" remote set-url origin "$REPO_URL" || true
exec "$DEST/deploy/native-server/bootstrap.sh"
