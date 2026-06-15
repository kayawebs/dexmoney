#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if [[ -f ".env.docker" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env.docker"
  set +a
fi

MODE="${1:-dry-run}"
LIMIT="${2:-5000}"

if [[ "$MODE" != "dry-run" && "$MODE" != "apply" ]]; then
  cat >&2 <<EOF
Usage:
  $0 dry-run [limit]
  $0 apply [limit]

Examples:
  $0 dry-run 5000
  $0 apply 5000
EOF
  exit 1
fi

if [[ "$MODE" == "apply" ]]; then
  cargo run -p base-arb-recorder --bin classify_observed_pools -- \
    --limit "$LIMIT" \
    --status observed_only \
    --status unresolved \
    --status classified_observed_only \
    --apply
else
  cargo run -p base-arb-recorder --bin classify_observed_pools -- \
    --limit "$LIMIT" \
    --status observed_only \
    --status unresolved \
    --status classified_observed_only
fi
