#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Managed installs publish code and dependencies together; pulling this live
# directory directly skips that transaction and its review/consent boundary.
HERMES_HOME_DIR="${HERMES_HOME:-$HOME/.hermes}"
if [[ -n "${HERMES_PROFILE:-}" ]]; then
  HERMES_HOME_DIR="$HERMES_HOME_DIR/profiles/${HERMES_PROFILE}"
fi
PLUGIN_TARGET="$HERMES_HOME_DIR/plugins/hermes-lcm"
if [[ -d "$PLUGIN_TARGET" && "$(cd "$PLUGIN_TARGET" && pwd -P)" == "$REPO_ROOT" ]]; then
  if ! command -v hermes >/dev/null 2>&1; then
    echo "Managed installation requires the Hermes launcher; no checkout was changed." >&2
    exit 1
  fi
  exec hermes plugins update hermes-lcm "$@"
fi
if (( $# )); then
  echo "Update options apply to managed installations only." >&2
  exit 2
fi

if command -v git >/dev/null 2>&1; then
  git -C "$REPO_ROOT" pull --ff-only
fi

"$SCRIPT_DIR/install.sh"

echo "Update complete. Restart Hermes if it is running."
