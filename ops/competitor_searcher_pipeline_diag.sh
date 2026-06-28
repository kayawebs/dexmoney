#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ops/competitor_searcher_pipeline_diag.sh --tx-hash 0x...
  ops/competitor_searcher_pipeline_diag.sh --report-entry "..."

Options are passed through to competitor_searcher_pipeline_diag:
  --tx-hash 0x...              Competitor transaction hash.
  --report-entry TEXT          Text containing a 0x-prefixed tx hash.
  --output PATH                Optional output file. Defaults to stdout.
  --max-depth N                Max recognized cycle depth. Default: 4.
  --max-price-impact-bps N     Override local impact guard for shadow checks.
  --max-pool-state-age-ms N    Override pool active-state age guard.
  --min-expected-profit N      Override min expected profit in raw anchor units.
EOF
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 2
fi

for arg in "$@"; do
  case "${arg}" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

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

CARGO_BIN="${CARGO_BIN:-}"
if [[ -z "${CARGO_BIN}" ]]; then
  if command -v cargo >/dev/null 2>&1; then
    CARGO_BIN="$(command -v cargo)"
  elif [[ -x "${HOME}/.cargo/bin/cargo" ]]; then
    CARGO_BIN="${HOME}/.cargo/bin/cargo"
  elif [[ -x "/usr/local/cargo/bin/cargo" ]]; then
    CARGO_BIN="/usr/local/cargo/bin/cargo"
  else
    CARGO_BIN="cargo"
  fi
fi

exec "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_searcher_pipeline_diag -- "$@"
