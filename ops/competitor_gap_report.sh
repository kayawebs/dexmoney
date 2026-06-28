#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ops/competitor_gap_report.sh [options]

Options:
  --address ADDR            Competitor collector/profit address.
                            Default: 0x0629da86af5a4ae1ba5e1589b13702558d0fb056
  --lookback-blocks N       Recent block window. Default: 5000
  --limit N                 Max competitor txs to inspect per report. Default: 200
  --top N                   Top flow rows for flow probe. Default: 50
  --out-dir DIR             Report parent directory. Default: reports
  --hydrate                 Also run competitor_report with bounded hydration.
  --hydrate-limit N         competitor_report hydrate limit. Default: 1000
  --hydrate-days N          competitor_report days window. Default: 1
  --tar                     Create a .tgz archive and print REPORT_TGZ.
  -h, --help                Show this help.

Creates one timestamped directory with:
  summary.txt
  competitor-pool-gap.txt
  competitor-live-compare.txt
  competitor-flow-probe.txt
  optional competitor-report.txt
EOF
}

ADDRESS="${COMPETITOR_ADDRESS:-0x0629da86af5a4ae1ba5e1589b13702558d0fb056}"
LOOKBACK_BLOCKS="${LOOKBACK_BLOCKS:-5000}"
LIMIT="${LIMIT:-200}"
TOP="${TOP:-50}"
OUT_DIR="${OUT_DIR:-reports}"
HYDRATE="${HYDRATE:-0}"
HYDRATE_LIMIT="${HYDRATE_LIMIT:-1000}"
HYDRATE_DAYS="${HYDRATE_DAYS:-1}"
CREATE_TAR="${CREATE_TAR:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --address)
      [[ $# -ge 2 ]] || { echo "--address requires a value" >&2; exit 2; }
      ADDRESS="$2"
      shift 2
      ;;
    --lookback-blocks)
      [[ $# -ge 2 ]] || { echo "--lookback-blocks requires a value" >&2; exit 2; }
      LOOKBACK_BLOCKS="$2"
      shift 2
      ;;
    --limit)
      [[ $# -ge 2 ]] || { echo "--limit requires a value" >&2; exit 2; }
      LIMIT="$2"
      shift 2
      ;;
    --top)
      [[ $# -ge 2 ]] || { echo "--top requires a value" >&2; exit 2; }
      TOP="$2"
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || { echo "--out-dir requires a value" >&2; exit 2; }
      OUT_DIR="$2"
      shift 2
      ;;
    --hydrate)
      HYDRATE="1"
      shift
      ;;
    --hydrate-limit)
      [[ $# -ge 2 ]] || { echo "--hydrate-limit requires a value" >&2; exit 2; }
      HYDRATE_LIMIT="$2"
      shift 2
      ;;
    --hydrate-days)
      [[ $# -ge 2 ]] || { echo "--hydrate-days requires a value" >&2; exit 2; }
      HYDRATE_DAYS="$2"
      shift 2
      ;;
    --tar)
      CREATE_TAR="1"
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

DB_URL="${POSTGRES_URL:-${DATABASE_URL:-postgres://user:password@127.0.0.1:5632/base_arb}}"
RPC_URL="${BASE_RPC_HTTP:-http://127.0.0.1:8545}"
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
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_NAME="competitor-gap-${STAMP}"
RUN_DIR="${OUT_DIR%/}/${RUN_NAME}"
SUMMARY="${RUN_DIR}/summary.txt"

mkdir -p "${RUN_DIR}"

section() {
  {
    echo
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
  } >>"${SUMMARY}"
}

run_cmd() {
  local title="$1"
  shift
  section "$title"
  {
    echo "+ $*"
    "$@"
    echo "status=$?"
  } >>"${SUMMARY}" 2>&1 || {
    local status=$?
    echo "status=${status}" >>"${SUMMARY}"
    return 0
  }
}

run_sql() {
  local title="$1"
  shift
  section "$title"
  psql "${DB_URL}" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"${SUMMARY}" 2>&1 || true
}

{
  echo "competitor gap report"
  echo "created_at_utc=${STAMP}"
  echo "address=${ADDRESS}"
  echo "lookback_blocks=${LOOKBACK_BLOCKS}"
  echo "limit=${LIMIT}"
  echo "top=${TOP}"
  echo "hydrate=${HYDRATE}"
  echo "rpc_url=${RPC_URL}"
  echo "db_url=${DB_URL}"
  echo "cargo_bin=${CARGO_BIN}"
  echo "run_dir=${RUN_DIR}"
} >"${SUMMARY}"

run_cmd "git state" git log -1 --oneline
run_cmd "local node height" bash -lc "curl -s -X POST -H 'Content-Type: application/json' --data '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_blockNumber\",\"params\":[]}' '${RPC_URL}'"

run_cmd "competitor pool gap" "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_pool_gap -- \
  --address "${ADDRESS}" \
  --lookback-blocks "${LOOKBACK_BLOCKS}" \
  --limit "${LIMIT}" \
  --include-opportunity-lookup \
  --output "${RUN_DIR}/competitor-pool-gap.txt"

run_cmd "competitor live compare" "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_live_compare -- \
  --address "${ADDRESS}" \
  --lookback-blocks "${LOOKBACK_BLOCKS}" \
  --limit "${LIMIT}" \
  --include-opportunity-lookup \
  --output "${RUN_DIR}/competitor-live-compare.txt"

run_cmd "competitor flow probe" "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_flow_probe -- \
  --address "${ADDRESS}" \
  --lookback-blocks "${LOOKBACK_BLOCKS}" \
  --limit "${LIMIT}" \
  --top "${TOP}" \
  --output "${RUN_DIR}/competitor-flow-probe.txt"

if [[ "${HYDRATE}" == "1" ]]; then
  run_cmd "competitor report bounded hydrate" "${CARGO_BIN}" run -p base-arb-recorder --bin competitor_report -- \
    --address "${ADDRESS}" \
    --days "${HYDRATE_DAYS}" \
    --hydrate-limit "${HYDRATE_LIMIT}" \
    --hydrate-peer-blocks 20 \
    --output "${RUN_DIR}/competitor-report.txt"
fi

run_sql "local bot recent activity" <<'SQL'
WITH windows(name, interval_text) AS (
  VALUES
    ('30m', '30 minutes'),
    ('2h', '2 hours'),
    ('12h', '12 hours')
)
SELECT
  w.name,
  (SELECT count(*) FROM opportunities WHERE created_at >= now() - w.interval_text::interval) AS opportunities,
  (SELECT count(*) FROM simulations WHERE created_at >= now() - w.interval_text::interval) AS simulations,
  (SELECT count(*) FROM transactions WHERE created_at >= now() - w.interval_text::interval) AS transactions,
  (SELECT max(created_at) FROM opportunities WHERE created_at >= now() - w.interval_text::interval) AS latest_opportunity,
  (SELECT max(created_at) FROM simulations WHERE created_at >= now() - w.interval_text::interval) AS latest_simulation,
  (SELECT max(created_at) FROM transactions WHERE created_at >= now() - w.interval_text::interval) AS latest_transaction
FROM windows w;
SQL

run_sql "recent simulation failure buckets" <<'SQL'
SELECT
  COALESCE(revert_reason, 'success') AS reason,
  count(*) AS n,
  max(created_at) AS latest
FROM simulations
WHERE created_at >= now() - interval '2 hours'
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
SQL

run_sql "Balancer V3 coverage summary" <<'SQL'
SELECT
  p.enabled,
  count(DISTINCT lower(p.pool_address)) AS pools,
  count(DISTINCT lower(pmc.pool_address)) AS model_coverage_pools,
  count(DISTINCT lower(pqc.pool_address)) AS quote_coverage_pools,
  max(p.updated_at) AS latest_pool_update
FROM pools p
LEFT JOIN pool_model_coverage pmc
  ON lower(pmc.pool_address) = lower(p.pool_address)
LEFT JOIN pool_quote_coverage pqc
  ON lower(pqc.pool_address) = lower(p.pool_address)
WHERE p.variant = 'BalancerV3'
GROUP BY p.enabled
ORDER BY p.enabled DESC;
SQL

run_sql "enabled Balancer V3 pools missing coverage" <<'SQL'
SELECT
  lower(p.pool_address) AS pool,
  p.token0,
  p.token1,
  p.fee_bps,
  p.source,
  CASE WHEN pmc.pool_address IS NULL THEN false ELSE true END AS has_model_coverage,
  CASE WHEN pqc.pool_address IS NULL THEN false ELSE true END AS has_quote_coverage,
  p.updated_at
FROM pools p
LEFT JOIN (
  SELECT DISTINCT lower(pool_address) AS pool_address FROM pool_model_coverage
) pmc ON pmc.pool_address = lower(p.pool_address)
LEFT JOIN (
  SELECT DISTINCT lower(pool_address) AS pool_address FROM pool_quote_coverage
) pqc ON pqc.pool_address = lower(p.pool_address)
WHERE p.enabled
  AND p.variant = 'BalancerV3'
  AND (pmc.pool_address IS NULL OR pqc.pool_address IS NULL)
ORDER BY p.updated_at DESC
LIMIT 50;
SQL

run_sql "opportunities by protocol combo" <<'SQL'
WITH recent AS (
  SELECT
    created_at,
    expected_profit,
    path_json::text AS path_text
  FROM opportunities
  WHERE created_at >= now() - interval '2 hours'
)
SELECT
  CASE
    WHEN path_text ILIKE '%BalancerV3%' THEN 'has_balancer_v3'
    WHEN path_text ILIKE '%UniswapV4%' OR path_text ILIKE '%uni-v4%' THEN 'has_uniswap_v4'
    WHEN path_text ILIKE '%AerodromeSlipstream%' THEN 'has_aero_slipstream'
    WHEN path_text ILIKE '%UniswapV3%' OR path_text ILIKE '%PancakeV3%' THEN 'v3_style_only'
    ELSE 'other_or_unknown'
  END AS bucket,
  count(*) AS opportunities,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_profit,
  max(expected_profit::numeric) AS max_profit
FROM recent
GROUP BY 1
ORDER BY opportunities DESC;
SQL

if [[ -f "${RUN_DIR}/competitor-pool-gap.txt" ]]; then
  section "pool gap counts excerpt"
  grep -n -A40 -E 'pool_gap_counts|protocol_counts|pool coverage rows|top' "${RUN_DIR}/competitor-pool-gap.txt" >>"${SUMMARY}" 2>&1 || true
fi

if [[ -f "${RUN_DIR}/competitor-live-compare.txt" ]]; then
  section "live compare excerpt"
  grep -n -A60 -E 'reason_counts|recognized|coverage|top|summary' "${RUN_DIR}/competitor-live-compare.txt" >>"${SUMMARY}" 2>&1 || true
fi

if [[ "${CREATE_TAR}" == "1" ]]; then
  TAR_PATH="${OUT_DIR%/}/${RUN_NAME}.tgz"
  tar -czf "${TAR_PATH}" -C "${OUT_DIR%/}" "${RUN_NAME}"
  echo "REPORT_TGZ=${TAR_PATH}"
fi

echo "REPORT_DIR=${RUN_DIR}"
echo "SUMMARY=${SUMMARY}"
