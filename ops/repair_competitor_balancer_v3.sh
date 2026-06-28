#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ops/repair_competitor_balancer_v3.sh [options]

Options:
  --report-dir DIR       Directory containing competitor-pool-gap.txt.
                         Default: newest reports/competitor-gap-* directory.
  --report FILE          competitor-pool-gap.txt path.
  --pool ADDR            Balancer pool address to classify/validate. Repeatable.
  --apply                Write pool_model_coverage and pool_quote_coverage.
  --limit N              Max pools passed to each recorder bin. Default: 1000.
  --amount-denom N       Validation amount = 10^decimals / N. Default: 100.
  --no-refresh-existing  Do not refresh existing model coverage rows.
  -h, --help             Show this help.

This script is intentionally separate from competitor_gap_report.sh. Reports stay
read-only; this repair script mutates coverage only with --apply.
EOF
}

REPORT_DIR=""
REPORT_FILE=""
APPLY=0
LIMIT=1000
AMOUNT_DENOM=100
REFRESH_EXISTING=1
POOLS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --report-dir)
      [[ $# -ge 2 ]] || { echo "--report-dir requires a value" >&2; exit 2; }
      REPORT_DIR="$2"
      shift 2
      ;;
    --report)
      [[ $# -ge 2 ]] || { echo "--report requires a value" >&2; exit 2; }
      REPORT_FILE="$2"
      shift 2
      ;;
    --pool)
      [[ $# -ge 2 ]] || { echo "--pool requires a value" >&2; exit 2; }
      POOLS+=("$(printf '%s' "$2" | tr '[:upper:]' '[:lower:]')")
      shift 2
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --limit)
      [[ $# -ge 2 ]] || { echo "--limit requires a value" >&2; exit 2; }
      LIMIT="$2"
      shift 2
      ;;
    --amount-denom)
      [[ $# -ge 2 ]] || { echo "--amount-denom requires a value" >&2; exit 2; }
      AMOUNT_DENOM="$2"
      shift 2
      ;;
    --no-refresh-existing)
      REFRESH_EXISTING=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
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

if [[ -z "${REPORT_FILE}" ]]; then
  if [[ -z "${REPORT_DIR}" ]]; then
    REPORT_DIR="$(find reports -maxdepth 1 -type d -name 'competitor-gap-*' 2>/dev/null | sort | tail -n 1 || true)"
  fi
  if [[ -n "${REPORT_DIR}" ]]; then
    REPORT_FILE="${REPORT_DIR%/}/competitor-pool-gap.txt"
  fi
fi

if [[ ${#POOLS[@]} -eq 0 && -n "${REPORT_FILE}" && -f "${REPORT_FILE}" ]]; then
  while IFS= read -r pool; do
    [[ -n "${pool}" ]] && POOLS+=("${pool}")
  done < <(
    awk -F '\t' '
      /^0x[0-9a-fA-F]{40}\t/ {
        topic = $4
        gap = $5
        if (topic == "balancer_v3" || gap ~ /^balancer_v3/) {
          print tolower($1)
        }
      }
    ' "${REPORT_FILE}" | sort -u
  )
fi

if [[ ${#POOLS[@]} -eq 0 ]]; then
  echo "no Balancer V3 pools found; pass --pool or --report-dir/--report" >&2
  exit 1
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

POOL_ARGS=()
for pool in "${POOLS[@]}"; do
  POOL_ARGS+=(--pool "${pool}")
done

APPLY_ARGS=()
if [[ "${APPLY}" == "1" ]]; then
  APPLY_ARGS+=(--apply)
fi

MODEL_ARGS=(--limit "${LIMIT}" "${POOL_ARGS[@]}")
if [[ "${REFRESH_EXISTING}" == "1" ]]; then
  MODEL_ARGS+=(--refresh-existing)
fi
MODEL_ARGS+=("${APPLY_ARGS[@]}")

QUOTE_ARGS=(--limit "${LIMIT}" --amount-denom "${AMOUNT_DENOM}" "${POOL_ARGS[@]}" "${APPLY_ARGS[@]}")

echo "competitor Balancer V3 coverage repair"
echo "apply=${APPLY}"
echo "report=${REPORT_FILE:-"-"}"
echo "pools=${#POOLS[@]}"
printf 'pool=%s\n' "${POOLS[@]}"

echo
echo "== classify_balancer_v3_models =="
"${CARGO_BIN}" run -p base-arb-recorder --bin classify_balancer_v3_models -- "${MODEL_ARGS[@]}"

echo
echo "== validate_balancer_v3_quotes =="
"${CARGO_BIN}" run -p base-arb-recorder --bin validate_balancer_v3_quotes -- "${QUOTE_ARGS[@]}"
