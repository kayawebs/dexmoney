#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ops/competitor_shadow_capital_loop.sh [options]

Runs a read-only competitor shadow-capital diagnostic loop:
  1. Estimates recent competitor WETH/USDC capital.
  2. Generates a competitor gap report.
  3. Replays sampled competitor txs through local searcher quote pipeline with shadow capital tiers.

This does not publish candidates, submit txs, restart services, or mutate live runtime state.

Options:
  --address ADDR             Competitor collector. Default: target competitor.
  --lookback-blocks N        Gap report block window. Default: 1000.
  --limit N                  Gap report tx limit. Default: 50.
  --top N                    Gap report top rows. Default: 20.
  --tx-limit N               Max txs to pipeline-diagnose per loop. Default: 20.
  --capital-lookback-blocks N
                             Live RPC block window for WETH/USDC capital estimate. Default: 5000.
  --capital-days N           Legacy DB capital window if live lookback is set to 0. Default: 1.
  --capital-limit N          Max capital txs to inspect. Default: 5000.
  --shadow-source max|p99|p90
                             Which competitor capital statistic to use as max tier. Default: max.
  --interval-secs N          Sleep between loops. Default: 1800.
  --hours N                  Stop after roughly N hours. Default: 12.
  --iterations N             Exact loop count. Overrides --hours.
  --out-dir DIR              Report parent directory. Default: reports.
  --once                     Run one loop and exit.
  -h, --help                 Show help.

Example:
  nohup ops/competitor_shadow_capital_loop.sh --hours 10 --lookback-blocks 1000 --limit 80 > shadow-capital-loop.log 2>&1 &
EOF
}

ADDRESS="${COMPETITOR_ADDRESS:-0x0629da86af5a4ae1ba5e1589b13702558d0fb056}"
LOOKBACK_BLOCKS="${LOOKBACK_BLOCKS:-1000}"
LIMIT="${LIMIT:-50}"
TOP="${TOP:-20}"
TX_LIMIT="${TX_LIMIT:-20}"
CAPITAL_LOOKBACK_BLOCKS="${CAPITAL_LOOKBACK_BLOCKS:-5000}"
CAPITAL_DAYS="${CAPITAL_DAYS:-1}"
CAPITAL_LIMIT="${CAPITAL_LIMIT:-5000}"
SHADOW_SOURCE="${SHADOW_SOURCE:-max}"
INTERVAL_SECS="${INTERVAL_SECS:-1800}"
HOURS="${HOURS:-12}"
ITERATIONS="${ITERATIONS:-}"
OUT_DIR="${OUT_DIR:-reports}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --address)
      ADDRESS="$2"; shift 2 ;;
    --lookback-blocks)
      LOOKBACK_BLOCKS="$2"; shift 2 ;;
    --limit)
      LIMIT="$2"; shift 2 ;;
    --top)
      TOP="$2"; shift 2 ;;
    --tx-limit)
      TX_LIMIT="$2"; shift 2 ;;
    --capital-days)
      CAPITAL_DAYS="$2"; shift 2 ;;
    --capital-lookback-blocks)
      CAPITAL_LOOKBACK_BLOCKS="$2"; shift 2 ;;
    --capital-limit)
      CAPITAL_LIMIT="$2"; shift 2 ;;
    --shadow-source)
      SHADOW_SOURCE="$2"; shift 2 ;;
    --interval-secs)
      INTERVAL_SECS="$2"; shift 2 ;;
    --hours)
      HOURS="$2"; shift 2 ;;
    --iterations)
      ITERATIONS="$2"; shift 2 ;;
    --out-dir)
      OUT_DIR="$2"; shift 2 ;;
    --once)
      ITERATIONS="1"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2 ;;
  esac
done

case "${SHADOW_SOURCE}" in
  max|p99|p90) ;;
  *)
    echo "--shadow-source must be max, p99, or p90" >&2
    exit 2
    ;;
esac

if [[ -z "${ITERATIONS}" ]]; then
  ITERATIONS=$(( (HOURS * 3600 + INTERVAL_SECS - 1) / INTERVAL_SECS ))
fi

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

read_env_value() {
  local file="$1"
  local key="$2"
  awk -F= -v k="${key}" '$1 == k {print $2}' "${file}" | tail -n 1
}

capital_key() {
  local token="$1"
  case "${SHADOW_SOURCE}" in
    max) echo "SHADOW_${token}_MAX_RAW" ;;
    p99) echo "SHADOW_${token}_P99_RAW" ;;
    p90) echo "SHADOW_${token}_P90_RAW" ;;
  esac
}

extract_report_txs() {
  local run_dir="$1"
  grep -REho '0x[0-9a-fA-F]{64}' "${run_dir}"/*.txt 2>/dev/null \
    | awk '!seen[tolower($0)]++ {print tolower($0)}' \
    | head -n "${TX_LIMIT}"
}

run_once() {
  local stamp run_dir summary capital_file gap_parent gap_dir usdc_key weth_key usdc_amount weth_amount tx_count
  local -a capital_args
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  run_dir="${OUT_DIR%/}/shadow-capital-${stamp}"
  summary="${run_dir}/shadow-summary.txt"
  capital_file="${run_dir}/shadow-capital.env"
  gap_parent="${run_dir}/gap"
  mkdir -p "${run_dir}" "${gap_parent}"

  {
    echo "shadow capital diagnostic"
    echo "created_at_utc=${stamp}"
    echo "address=${ADDRESS}"
    echo "lookback_blocks=${LOOKBACK_BLOCKS}"
    echo "limit=${LIMIT}"
    echo "top=${TOP}"
    echo "tx_limit=${TX_LIMIT}"
    echo "capital_lookback_blocks=${CAPITAL_LOOKBACK_BLOCKS}"
    echo "capital_days=${CAPITAL_DAYS}"
    echo "capital_limit=${CAPITAL_LIMIT}"
    echo "shadow_source=${SHADOW_SOURCE}"
    echo "run_dir=${run_dir}"
  } >"${summary}"

  capital_args=(
    --address "${ADDRESS}"
    --limit "${CAPITAL_LIMIT}"
    --shell
    --output "${capital_file}"
  )
  if [[ "${CAPITAL_LOOKBACK_BLOCKS}" != "0" ]]; then
    capital_args+=(--live-lookback-blocks "${CAPITAL_LOOKBACK_BLOCKS}")
  else
    capital_args+=(--days "${CAPITAL_DAYS}")
  fi
  "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_capital -- \
    "${capital_args[@]}" >>"${summary}" 2>&1

  usdc_key="$(capital_key USDC)"
  weth_key="$(capital_key WETH)"
  usdc_amount="$(read_env_value "${capital_file}" "${usdc_key}")"
  weth_amount="$(read_env_value "${capital_file}" "${weth_key}")"
  usdc_amount="${usdc_amount:-0}"
  weth_amount="${weth_amount:-0}"

  {
    echo
    echo "== Shadow Amounts =="
    echo "usdc_key=${usdc_key}"
    echo "usdc_shadow_max_raw=${usdc_amount}"
    echo "weth_key=${weth_key}"
    echo "weth_shadow_max_raw=${weth_amount}"
  } >>"${summary}"

  ./ops/competitor_gap_report.sh \
    --address "${ADDRESS}" \
    --lookback-blocks "${LOOKBACK_BLOCKS}" \
    --limit "${LIMIT}" \
    --top "${TOP}" \
    --out-dir "${gap_parent}" >>"${summary}" 2>&1

  gap_dir="$(find "${gap_parent}" -maxdepth 1 -type d -name 'competitor-gap-*' | sort | tail -n 1 || true)"
  echo "gap_dir=${gap_dir}" >>"${summary}"
  if [[ -z "${gap_dir}" ]]; then
    echo "status=gap_report_missing" >>"${summary}"
    echo "REPORT_DIR=${run_dir}"
    return 0
  fi

  tx_count=0
  while read -r tx_hash; do
    [[ -n "${tx_hash}" ]] || continue
    tx_count=$((tx_count + 1))
    local short out args
    short="${tx_hash:2:8}"
    out="${run_dir}/pipeline-${tx_count}-${short}.txt"
    args=(--tx-hash "${tx_hash}" --output "${out}")
    if [[ "${usdc_amount}" != "0" ]]; then
      args+=(--shadow-token-max "USDC:${usdc_amount}")
    fi
    if [[ "${weth_amount}" != "0" ]]; then
      args+=(--shadow-token-max "WETH:${weth_amount}")
    fi
    ./ops/competitor_searcher_pipeline_diag.sh "${args[@]}" >>"${summary}" 2>&1 || true
  done < <(extract_report_txs "${gap_dir}")

  {
    echo
    echo "== Pipeline Summary =="
    echo "pipeline_tx_count=${tx_count}"
    if ls "${run_dir}"/pipeline-*.txt >/dev/null 2>&1; then
      grep -hE '^(root_stage|root_detail):' "${run_dir}"/pipeline-*.txt || true
      echo
      echo "shadow_passes=$(grep -h 'stage=shadow_would_publish_if_amount_were_in_grid' "${run_dir}"/pipeline-*.txt 2>/dev/null | wc -l | tr -d ' ')"
      echo "candidate_grid_passes=$(grep -h 'stage=candidate_publish_eligible' "${run_dir}"/pipeline-*.txt 2>/dev/null | wc -l | tr -d ' ')"
      echo "quote_errors=$(grep -h 'stage=quote_error' "${run_dir}"/pipeline-*.txt 2>/dev/null | wc -l | tr -d ' ')"
      echo "missing_ticks=$(grep -h 'quote_skipped reason=MissingTicks' "${run_dir}"/pipeline-*.txt 2>/dev/null | wc -l | tr -d ' ')"
      echo "tick_range_exhausted=$(grep -h 'quote_skipped reason=TickRangeExhausted' "${run_dir}"/pipeline-*.txt 2>/dev/null | wc -l | tr -d ' ')"
    else
      echo "no pipeline reports generated"
    fi
  } >>"${summary}"

  echo "REPORT_DIR=${run_dir}"
  echo "SUMMARY=${summary}"
}

echo "competitor shadow capital loop starting iterations=${ITERATIONS} interval_secs=${INTERVAL_SECS}"
for ((i = 1; i <= ITERATIONS; i++)); do
  echo "loop=${i}/${ITERATIONS} start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  run_once
  if (( i < ITERATIONS )); then
    sleep "${INTERVAL_SECS}"
  fi
done
