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

INTERVAL="${INTERVAL:-12 hours}"
HUB_ADDRESS="${HUB_ADDRESS:-${1:-${EXECUTOR_CONTRACT:-}}}"
OUT_DIR="${2:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/hub-submitted-failure-diag-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ -z "$HUB_ADDRESS" ]]; then
  cat >&2 <<EOF
usage:
  INTERVAL="12 hours" HUB_ADDRESS=0x... $0 [hub_address] [out_dir]

This report diagnoses submitted transactions for the Hub from the local
transactions table. observed_transactions is used only as an optional chain
index supplement because the chain indexer may lag or omit our own txs.
EOF
  exit 2
fi

mkdir -p "$OUT_DIR"

psql_check() {
  psql "$DB_URL" -X -q -Atc "SELECT 1" >/dev/null
}

if ! psql_check; then
  cat >&2 <<EOF
failed to connect database.

Current DB URL:
  $DB_URL

Set one of:
  POSTGRES_URL=postgres://user:password@127.0.0.1:5632/base_arb
  DATABASE_URL=postgres://user:password@127.0.0.1:5632/base_arb

Then rerun:
  INTERVAL="$INTERVAL" HUB_ADDRESS="$HUB_ADDRESS" $0
EOF
  exit 1
fi

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
    "$@" >>"$OUT_FILE"
}

{
  echo "hub submitted failure diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "hub: $HUB_ADDRESS"
  echo "database: $DB_URL"
  echo
  echo "scope: submitted transactions only; simulations without tx_hash are excluded from failure ratios"
  echo "primary_source: transactions.tx_hash; observed_transactions is supplementary"
} >"$OUT_FILE"

run_sql "0. database freshness" <<'SQL'
SELECT now() AS db_now, current_database() AS db, current_user AS "user";

SELECT 'opportunities' AS table_name, count(*) AS n, max(created_at) AS latest
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
UNION ALL
SELECT 'simulations', count(*), max(created_at)
FROM simulations
WHERE created_at >= now() - :'interval'::interval
UNION ALL
SELECT 'transactions_with_hash', count(*), max(created_at)
FROM transactions
WHERE created_at >= now() - :'interval'::interval
  AND tx_hash IS NOT NULL
UNION ALL
SELECT 'observed_hub_txs', count(*), max(updated_at)
FROM observed_transactions
WHERE lower(to_address) = lower(:'hub')
  AND updated_at >= now() - :'interval'::interval
ORDER BY table_name;
SQL

run_sql "1. submitted funnel" <<'SQL'
WITH hub_chain AS (
  SELECT *
  FROM observed_transactions
  WHERE lower(to_address) = lower(:'hub')
    AND updated_at >= now() - :'interval'::interval
),
db_tx AS (
  SELECT *
  FROM transactions
  WHERE created_at >= now() - :'interval'::interval
    AND tx_hash IS NOT NULL
),
db_hub AS (
  SELECT t.*
  FROM db_tx t
  JOIN hub_chain ot ON lower(ot.tx_hash) = lower(t.tx_hash)
)
SELECT 'db_tx_with_hash' AS name, count(*) AS n, max(created_at) AS latest
FROM db_tx
UNION ALL
SELECT 'observed_hub_txs', count(*), max(updated_at)
FROM hub_chain
UNION ALL
SELECT 'observed_hub_success', count(*), max(updated_at)
FROM hub_chain
WHERE status IS TRUE
UNION ALL
SELECT 'observed_hub_reverted', count(*), max(updated_at)
FROM hub_chain
WHERE status IS FALSE
UNION ALL
SELECT 'observed_hub_unknown_status', count(*), max(updated_at)
FROM hub_chain
WHERE status IS NULL
UNION ALL
SELECT 'db_tx_joined_to_hub_chain', count(*), max(created_at)
FROM db_hub
UNION ALL
SELECT 'db_tx_not_observed_yet', count(*), max(t.created_at)
FROM db_tx t
LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
WHERE ot.tx_hash IS NULL
ORDER BY name;
SQL

run_sql "2. submitted tx status, simulation reason, and gas" <<'SQL'
WITH submitted AS (
  SELECT
    t.tx_hash,
    COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
    ot.transaction_index,
    COALESCE(ot.updated_at, t.created_at) AS observed_at,
    CASE
      WHEN ot.status IS TRUE OR t.status = 'Confirmed' THEN 'success'
      WHEN ot.status IS FALSE OR t.status = 'Reverted' THEN 'reverted'
      ELSE 'unknown'
    END AS chain_status,
    COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric) AS gas_used,
    COALESCE(
      NULLIF(ot.effective_gas_price, '')::numeric,
      NULLIF(t.effective_gas_price, '')::numeric
    ) AS effective_gas_price,
    t.status AS db_status,
    t.created_at AS db_recorded_at,
    s.success AS sim_success,
    CASE
      WHEN s.success THEN 'sim_success'
      WHEN s.revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN s.revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN s.revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
      WHEN s.revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
      WHEN s.revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      WHEN s.revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
      WHEN s.revert_reason IS NULL OR s.revert_reason = '' THEN '-'
      ELSE s.revert_reason
    END AS sim_reason
  FROM transactions t
  LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
  LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
  LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
  WHERE t.created_at >= now() - :'interval'::interval
    AND t.tx_hash IS NOT NULL
)
SELECT
  chain_status,
  COALESCE(db_status, '-') AS db_status,
  COALESCE(sim_reason, '-') AS sim_reason,
  count(DISTINCT tx_hash) AS txs,
  max(block_number) AS latest_block,
  max(observed_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY gas_used) AS p50_gas_used,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY gas_used) AS p90_gas_used,
  max(gas_used) AS max_gas_used,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY gas_used * effective_gas_price) AS p50_fee_wei,
  max(gas_used * effective_gas_price) AS max_fee_wei
FROM submitted
GROUP BY 1, 2, 3
ORDER BY chain_status DESC, txs DESC, max_fee_wei DESC NULLS LAST
LIMIT 80;
SQL

run_sql "3. reverted submitted tx details" <<'SQL'
WITH submitted AS (
  SELECT
    t.tx_hash,
    COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
    ot.transaction_index,
    COALESCE(ot.from_address, t.eoa) AS from_address,
    COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric) AS gas_used_num,
    COALESCE(
      NULLIF(ot.effective_gas_price, '')::numeric,
      NULLIF(t.effective_gas_price, '')::numeric
    ) AS effective_gas_price_num,
    t.id AS tx_record_id,
    t.created_at AS tx_recorded_at,
    t.opportunity_id,
    t.simulation_id,
    t.eoa,
    t.status AS db_status,
    t.revert_reason AS tx_revert_reason,
    s.success AS sim_success,
    s.revert_reason AS sim_revert_reason,
    s.path_name AS sim_path_name,
    o.path_json,
    o.token_in,
    o.amount_in,
    o.expected_profit,
    o.min_profit
  FROM transactions t
  LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
  LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
  LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
  WHERE t.created_at >= now() - :'interval'::interval
    AND t.tx_hash IS NOT NULL
    AND (
      ot.status IS FALSE
      OR (ot.tx_hash IS NULL AND t.status = 'Reverted')
    )
)
SELECT
  block_number,
  transaction_index,
  tx_hash,
  from_address,
  COALESCE(db_status, '-') AS db_status,
  gas_used_num AS gas_used,
  effective_gas_price_num AS effective_gas_price,
  gas_used_num * effective_gas_price_num AS fee_wei,
  COALESCE(sim_path_name, path_json->>'name', '-') AS path_name,
  token_in,
  amount_in,
  expected_profit,
  min_profit,
  CASE
    WHEN sim_success THEN 'sim_success'
    WHEN sim_revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
    WHEN sim_revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
    WHEN sim_revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
    WHEN sim_revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
    WHEN sim_revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
    WHEN sim_revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
    ELSE COALESCE(NULLIF(sim_revert_reason, ''), NULLIF(tx_revert_reason, ''), '-')
  END AS sim_or_tx_reason,
  (
    SELECT string_agg(
      COALESCE(step->>'dex', '') || ':' ||
      COALESCE(step->>'variant', '') || ':' ||
      right(COALESCE(step->>'pool', step->>'pool_address', ''), 6),
      ' -> '
      ORDER BY ord
    )
    FROM jsonb_array_elements(COALESCE(path_json->'steps', '[]'::jsonb)) WITH ORDINALITY AS x(step, ord)
  ) AS path_signature
FROM submitted
ORDER BY block_number DESC, transaction_index DESC
LIMIT 200;
SQL

run_sql "4. reverted submitted txs by path and reason" <<'SQL'
WITH submitted AS (
  SELECT
    t.tx_hash,
    COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
    COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric) AS gas_used,
    COALESCE(
      NULLIF(ot.effective_gas_price, '')::numeric,
      NULLIF(t.effective_gas_price, '')::numeric
    ) AS effective_gas_price,
    s.success AS sim_success,
    s.revert_reason AS sim_revert_reason,
    COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
    o.token_in,
    o.amount_in,
    o.expected_profit,
    o.min_profit,
    o.path_json
  FROM transactions t
  LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
  LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
  LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
  WHERE t.created_at >= now() - :'interval'::interval
    AND t.tx_hash IS NOT NULL
    AND (
      ot.status IS FALSE
      OR (ot.tx_hash IS NULL AND t.status = 'Reverted')
    )
)
SELECT
  CASE
    WHEN path_json::text ILIKE '%UniswapV4%' OR path_name ILIKE '%uni-v4%' THEN 'has_uniswap_v4'
    WHEN path_json::text ILIKE '%BalancerV3%' OR path_name ILIKE '%balancer%' THEN 'has_balancer_v3'
    WHEN path_json::text ILIKE '%AerodromeSlipstream%' OR path_name ILIKE '%aero-slipstream%' THEN 'has_aero_slipstream'
    WHEN path_json::text ILIKE '%UniswapV3%' OR path_name ILIKE '%uni-v3%' THEN 'has_v3'
    ELSE 'other'
  END AS path_bucket,
  CASE
    WHEN sim_success THEN 'sim_success'
    WHEN sim_revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
    WHEN sim_revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
    WHEN sim_revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
    WHEN sim_revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
    WHEN sim_revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
    WHEN sim_revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
    ELSE COALESCE(NULLIF(sim_revert_reason, ''), '-')
  END AS reason,
  path_name,
  token_in,
  amount_in,
  count(DISTINCT tx_hash) AS reverted_txs,
  max(block_number) AS latest_block,
  max(NULLIF(expected_profit, '')::numeric) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY gas_used) AS p50_gas_used,
  max(gas_used) AS max_gas_used,
  max(gas_used * effective_gas_price) AS max_fee_wei
FROM submitted
GROUP BY 1, 2, 3, 4, 5
ORDER BY reverted_txs DESC, max_fee_wei DESC NULLS LAST
LIMIT 100;
SQL

run_sql "5. success vs revert gas comparison" <<'SQL'
WITH submitted AS (
  SELECT
    CASE
      WHEN ot.status IS TRUE OR t.status = 'Confirmed' THEN 'success'
      WHEN ot.status IS FALSE OR t.status = 'Reverted' THEN 'reverted'
      ELSE 'unknown'
    END AS chain_status,
    COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
    COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric) AS gas_used,
    COALESCE(
      NULLIF(ot.effective_gas_price, '')::numeric,
      NULLIF(t.effective_gas_price, '')::numeric
    ) AS effective_gas_price
  FROM transactions t
  LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
  LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
  LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
  WHERE t.created_at >= now() - :'interval'::interval
    AND t.tx_hash IS NOT NULL
)
SELECT
  chain_status,
  count(*) AS txs,
  max(block_number) AS latest_block,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY gas_used) AS p50_gas_used,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY gas_used) AS p90_gas_used,
  max(gas_used) AS max_gas_used,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY gas_used * effective_gas_price) AS p50_fee_wei,
  max(gas_used * effective_gas_price) AS max_fee_wei
FROM submitted
GROUP BY 1
ORDER BY txs DESC;
SQL

run_sql "6. approvals near reverted hub txs" <<'SQL'
WITH reverted AS (
  SELECT
    t.tx_hash,
    COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
    ot.transaction_index,
    COALESCE(ot.from_address, t.eoa) AS from_address
  FROM transactions t
  LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
  LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
  LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
  WHERE t.created_at >= now() - :'interval'::interval
    AND t.tx_hash IS NOT NULL
    AND (
      ot.status IS FALSE
      OR (ot.tx_hash IS NULL AND t.status = 'Reverted')
    )
)
SELECT
  r.block_number,
  r.transaction_index,
  r.tx_hash AS reverted_tx,
  r.from_address,
  count(a.tx_hash) AS approval_txs_prev_3_blocks,
  max(a.block_number) AS latest_approval_block,
  string_agg(a.tx_hash, ', ' ORDER BY a.block_number DESC, a.transaction_index DESC) AS approval_hashes
FROM reverted r
LEFT JOIN observed_transactions a
  ON lower(a.from_address) = lower(r.from_address)
 AND a.block_number BETWEEN r.block_number - 3 AND r.block_number
 AND lower(a.tx_hash) <> lower(r.tx_hash)
 AND (
   COALESCE(a.tx_json->>'input', a.tx_json->>'data', '') ILIKE '0x095ea7b3%'
   OR COALESCE(a.tx_json->>'input', a.tx_json->>'data', '') ILIKE '0xa9059cbb%'
 )
GROUP BY 1, 2, 3, 4
ORDER BY r.block_number DESC, r.transaction_index DESC
LIMIT 100;
SQL

run_sql "7. recent successful submitted txs for comparison" <<'SQL'
SELECT
  COALESCE(ot.block_number, s.block_number, o.block_number) AS block_number,
  ot.transaction_index,
  t.tx_hash,
  COALESCE(ot.from_address, t.eoa) AS from_address,
  COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric) AS gas_used,
  COALESCE(
    NULLIF(ot.effective_gas_price, '')::numeric,
    NULLIF(t.effective_gas_price, '')::numeric
  ) AS effective_gas_price,
  COALESCE(NULLIF(ot.gas_used, '')::numeric, NULLIF(t.gas_used, '')::numeric)
    * COALESCE(NULLIF(ot.effective_gas_price, '')::numeric, NULLIF(t.effective_gas_price, '')::numeric) AS fee_wei,
  COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
  o.token_in,
  o.amount_in,
  o.expected_profit,
  o.min_profit,
  t.realized_profit,
  t.status AS db_status
FROM transactions t
LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
WHERE t.created_at >= now() - :'interval'::interval
  AND t.tx_hash IS NOT NULL
  AND (
    ot.status IS TRUE
    OR (ot.tx_hash IS NULL AND t.status = 'Confirmed')
  )
ORDER BY ot.block_number DESC, ot.transaction_index DESC
LIMIT 100;
SQL

run_sql "8. DB tx hashes not observed yet" <<'SQL'
SELECT
  t.created_at,
  t.opportunity_id,
  t.simulation_id,
  t.eoa,
  t.tx_hash,
  t.nonce,
  t.status AS db_status,
  t.revert_reason,
  COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
  o.token_in,
  o.amount_in,
  o.expected_profit,
  o.min_profit
FROM transactions t
LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
LEFT JOIN simulations s ON s.id = t.simulation_id OR s.opportunity_id = t.opportunity_id
LEFT JOIN opportunities o ON o.id = COALESCE(t.opportunity_id, s.opportunity_id)
WHERE t.created_at >= now() - :'interval'::interval
  AND t.tx_hash IS NOT NULL
  AND ot.tx_hash IS NULL
ORDER BY t.created_at DESC
LIMIT 100;
SQL

run_sql "9. all simulation outcomes, including non-submitted" <<'SQL'
WITH sim_scope AS (
  SELECT
    s.id AS simulation_id,
    s.created_at,
    s.block_number,
    s.success,
    s.revert_reason,
    COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
    o.path_json,
    o.token_in,
    o.amount_in,
    o.expected_profit,
    o.min_profit,
    t.tx_hash,
    t.status AS tx_status
  FROM simulations s
  LEFT JOIN opportunities o ON o.id = s.opportunity_id
  LEFT JOIN transactions t ON t.simulation_id = s.id
  WHERE s.created_at >= now() - :'interval'::interval
),
classified AS (
  SELECT
    *,
    CASE
      WHEN path_json::text ILIKE '%UniswapV4%' OR path_name ILIKE '%uni-v4%' THEN 'has_uniswap_v4'
      WHEN path_json::text ILIKE '%BalancerV3%' OR path_name ILIKE '%balancer%' THEN 'has_balancer_v3'
      WHEN path_json::text ILIKE '%AerodromeSlipstream%' OR path_name ILIKE '%aero-slipstream%' THEN 'has_aero_slipstream'
      WHEN path_json::text ILIKE '%UniswapV3%' OR path_name ILIKE '%uni-v3%' THEN 'has_v3'
      ELSE 'other'
    END AS path_bucket,
    CASE
      WHEN success THEN 'success'
      WHEN revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
      WHEN revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
      WHEN revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      WHEN revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
      WHEN revert_reason IS NULL OR revert_reason = '' THEN '-'
      ELSE revert_reason
    END AS reason
  FROM sim_scope
)
SELECT
  path_bucket,
  reason,
  count(DISTINCT simulation_id) AS simulations,
  count(DISTINCT tx_hash) FILTER (WHERE tx_hash IS NOT NULL) AS submitted_txs,
  count(DISTINCT simulation_id) FILTER (WHERE tx_hash IS NULL) AS non_submitted_simulations,
  max(block_number) AS latest_block,
  max(created_at) AS latest,
  min(NULLIF(expected_profit, '')::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY NULLIF(expected_profit, '')::numeric) AS p50_expected_profit,
  max(NULLIF(expected_profit, '')::numeric) AS max_expected_profit
FROM classified
GROUP BY 1, 2
ORDER BY simulations DESC, max_expected_profit DESC NULLS LAST
LIMIT 100;
SQL

run_sql "10. top non-submitted simulation failures by path" <<'SQL'
WITH sim_scope AS (
  SELECT
    s.id AS simulation_id,
    s.created_at,
    s.block_number,
    s.success,
    s.revert_reason,
    COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
    o.path_json,
    o.token_in,
    o.amount_in,
    o.expected_profit,
    o.min_profit,
    t.tx_hash
  FROM simulations s
  LEFT JOIN opportunities o ON o.id = s.opportunity_id
  LEFT JOIN transactions t ON t.simulation_id = s.id
  WHERE s.created_at >= now() - :'interval'::interval
),
classified AS (
  SELECT
    *,
    CASE
      WHEN path_json::text ILIKE '%UniswapV4%' OR path_name ILIKE '%uni-v4%' THEN 'has_uniswap_v4'
      WHEN path_json::text ILIKE '%BalancerV3%' OR path_name ILIKE '%balancer%' THEN 'has_balancer_v3'
      WHEN path_json::text ILIKE '%AerodromeSlipstream%' OR path_name ILIKE '%aero-slipstream%' THEN 'has_aero_slipstream'
      WHEN path_json::text ILIKE '%UniswapV3%' OR path_name ILIKE '%uni-v3%' THEN 'has_v3'
      ELSE 'other'
    END AS path_bucket,
    CASE
      WHEN success THEN 'success'
      WHEN revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
      WHEN revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
      WHEN revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      WHEN revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
      WHEN revert_reason IS NULL OR revert_reason = '' THEN '-'
      ELSE revert_reason
    END AS reason
  FROM sim_scope
)
SELECT
  path_bucket,
  reason,
  path_name,
  token_in,
  amount_in,
  count(DISTINCT simulation_id) AS simulations,
  max(block_number) AS latest_block,
  max(created_at) AS latest,
  min(NULLIF(expected_profit, '')::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY NULLIF(expected_profit, '')::numeric) AS p50_expected_profit,
  max(NULLIF(expected_profit, '')::numeric) AS max_expected_profit,
  max(NULLIF(min_profit, '')::numeric) AS max_min_profit
FROM classified
WHERE success IS NOT TRUE
  AND tx_hash IS NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY simulations DESC, max_expected_profit DESC NULLS LAST
LIMIT 100;
SQL

echo "$OUT_FILE"
