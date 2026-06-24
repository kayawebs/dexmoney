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

INTERVAL="${INTERVAL:-30 minutes}"
HUB_ADDRESS="${HUB_ADDRESS:-${1:-${EXECUTOR_CONTRACT:-}}}"
OUT_DIR="${2:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/execution-hub-diag-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ -z "$HUB_ADDRESS" ]]; then
  echo "HUB_ADDRESS is required, or pass it as the first argument" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

section() {
  {
    echo
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
  } >>"$OUT_FILE"
}

run_sql() {
  local title="$1"
  shift
  section "$title"
  psql "$DB_URL" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --set=interval="$INTERVAL" \
    --set=hub="$HUB_ADDRESS" \
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE" 2>&1 || true
}

{
  echo "execution hub diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "hub: $HUB_ADDRESS"
  echo "database: $DB_URL"
} >"$OUT_FILE"

run_sql "1. Funnel" <<'SQL'
WITH params AS (
  SELECT lower(:'hub') AS hub
),
hub_chain AS (
  SELECT *
  FROM observed_transactions ot, params
  WHERE lower(ot.to_address) = params.hub
    AND ot.updated_at >= now() - :'interval'::interval
),
hub_db_tx AS (
  SELECT t.*
  FROM transactions t
  JOIN hub_chain ot ON lower(ot.tx_hash) = lower(t.tx_hash)
),
sim AS (
  SELECT *
  FROM simulations
  WHERE created_at >= now() - :'interval'::interval
)
SELECT 'opportunities' AS name, count(*) AS n, max(created_at) AS latest
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
UNION ALL
SELECT 'simulations', count(*), max(created_at)
FROM sim
UNION ALL
SELECT 'hub_chain_txs', count(*), max(updated_at)
FROM hub_chain
UNION ALL
SELECT 'hub_db_txs', count(*), max(created_at)
FROM hub_db_tx
ORDER BY name;
SQL

run_sql "2. Hub chain transaction status and gas" <<'SQL'
SELECT
  CASE status WHEN true THEN 'success' WHEN false THEN 'reverted' ELSE 'unknown' END AS chain_status,
  count(*) AS n,
  max(block_number) AS latest_block,
  max(updated_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY NULLIF(gas_used, '')::numeric) AS p50_gas_used,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY NULLIF(gas_used, '')::numeric) AS p90_gas_used,
  max(NULLIF(gas_used, '')::numeric) AS max_gas_used
FROM observed_transactions
WHERE lower(to_address) = lower(:'hub')
  AND updated_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC;
SQL

run_sql "3. Simulation reason summary" <<'SQL'
SELECT
  CASE
    WHEN success THEN 'success'
    WHEN revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
    WHEN revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
    WHEN revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
    WHEN revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
    WHEN revert_reason ILIKE '%trusted factory%' OR revert_reason ILIKE '%factory is not configured%' THEN 'untrusted_factory'
    WHEN revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
    WHEN revert_reason ILIKE '%0x5a7cfa65%' THEN 'router_selector_0x5a7cfa65'
    ELSE COALESCE(NULLIF(revert_reason, ''), 'unknown_failure')
  END AS reason,
  count(*) AS n,
  max(created_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY NULLIF(expected_profit, '')::numeric) AS p50_expected_profit,
  max(NULLIF(expected_profit, '')::numeric) AS max_expected_profit
FROM simulations
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC
LIMIT 30;
SQL

run_sql "4. Hub submitted txs joined to simulation" <<'SQL'
SELECT
  t.created_at AS tx_recorded_at,
  ot.block_number,
  CASE ot.status WHEN true THEN 'success' WHEN false THEN 'reverted' ELSE 'unknown' END AS chain_status,
  t.status AS db_status,
  t.tx_hash,
  t.eoa,
  t.nonce,
  ot.gas_used,
  ot.effective_gas_price,
  s.success AS sim_success,
  CASE
    WHEN s.success THEN 'success'
    WHEN s.revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
    WHEN s.revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
    WHEN s.revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
    WHEN s.revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
    WHEN s.revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
    ELSE COALESCE(NULLIF(s.revert_reason, ''), '-')
  END AS sim_reason,
  COALESCE(s.path_name, o.path_json->>'name') AS path_name,
  o.token_in,
  o.amount_in,
  o.expected_profit,
  o.min_profit
FROM observed_transactions ot
LEFT JOIN transactions t ON lower(t.tx_hash) = lower(ot.tx_hash)
LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
WHERE lower(ot.to_address) = lower(:'hub')
  AND ot.updated_at >= now() - :'interval'::interval
ORDER BY ot.block_number DESC, ot.transaction_index DESC, s.created_at DESC NULLS LAST
LIMIT 100;
SQL

run_sql "5. Submitted path summary" <<'SQL'
SELECT
  COALESCE(s.path_name, o.path_json->>'name') AS path_name,
  o.token_in,
  o.amount_in,
  count(*) AS txs,
  count(*) FILTER (WHERE ot.status IS TRUE) AS chain_success,
  count(*) FILTER (WHERE ot.status IS FALSE) AS chain_reverted,
  max(ot.block_number) AS latest_block,
  max(o.expected_profit) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY NULLIF(ot.gas_used, '')::numeric) AS p50_gas_used
FROM observed_transactions ot
LEFT JOIN transactions t ON lower(t.tx_hash) = lower(ot.tx_hash)
LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
LEFT JOIN opportunities o ON o.id = t.opportunity_id
WHERE lower(ot.to_address) = lower(:'hub')
  AND ot.updated_at >= now() - :'interval'::interval
GROUP BY 1, 2, 3
ORDER BY txs DESC, chain_reverted DESC
LIMIT 50;
SQL

run_sql "6. Recent high-value failed simulations" <<'SQL'
SELECT
  s.created_at,
  s.block_number,
  s.revert_reason,
  s.path_name,
  s.token_in,
  s.amount_in,
  s.expected_profit,
  s.min_profit
FROM simulations s
WHERE s.created_at >= now() - :'interval'::interval
  AND NOT s.success
ORDER BY NULLIF(s.expected_profit, '')::numeric DESC NULLS LAST
LIMIT 50;
SQL

run_sql "7. Recent successful simulations not submitted" <<'SQL'
SELECT
  s.created_at,
  s.block_number,
  s.path_name,
  s.token_in,
  s.amount_in,
  s.expected_profit,
  s.min_profit,
  t.status AS tx_status,
  t.tx_hash
FROM simulations s
LEFT JOIN transactions t ON t.simulation_id = s.id
WHERE s.created_at >= now() - :'interval'::interval
  AND s.success
  AND t.id IS NULL
ORDER BY s.created_at DESC
LIMIT 50;
SQL

echo "$OUT_FILE"
