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

APPLY="${APPLY:-1}"
GAPS_ONLY="${GAPS_ONLY:-1}"
LIMIT="${LIMIT:-500}"
MAX_AGE_HOURS="${MAX_AGE_HOURS:-24}"
WORD_RADIUS="${WORD_RADIUS:-8}"

args=(
  --limit "$LIMIT"
  --max-age-hours "$MAX_AGE_HOURS"
  --word-radius "$WORD_RADIUS"
)

if [[ "$APPLY" == "1" || "$APPLY" == "true" ]]; then
  args+=(--apply)
fi

if [[ "$GAPS_ONLY" == "1" || "$GAPS_ONLY" == "true" ]]; then
  args+=(--gaps-only)
fi

echo "running V3-style tick repair:"
printf '  cargo run -p base-arb-recorder --bin repair_v3_ticks --'
printf ' %q' "${args[@]}" "$@"
printf '\n'

cargo run -p base-arb-recorder --bin repair_v3_ticks -- "${args[@]}" "$@"
