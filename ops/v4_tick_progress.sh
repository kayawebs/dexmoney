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

DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
if [[ -z "${DB_URL}" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

LOG_FILE="${1:-v4-tick-full-single-pass.txt}"

echo "== V4 tick scan log =="
echo "file: ${LOG_FILE}"
if [[ -f "${LOG_FILE}" ]]; then
  grep -E "manager_tick_scan|pools=|failed=|Error:" "${LOG_FILE}" | tail -n 20 || true
else
  echo "missing log file"
fi

echo
echo "== V4 DB coverage =="
psql "${DB_URL}" \
  -X \
  --set=ON_ERROR_STOP=1 \
  --pset=pager=off \
  --pset=border=2 <<'SQL'
WITH v4 AS (
  SELECT
    lower(pool_address) AS pool_address,
    pool_uid,
    event_type,
    token0,
    token1,
    fee_pips,
    pool_key_fee_pips,
    tick_spacing,
    hooks_address,
    liquidity,
    sqrt_price_x96,
    tick,
    latest_block
  FROM protocol_pool_observations
  WHERE protocol = 'uniswap-v4'
    AND pool_address IS NOT NULL
),
v4_pools AS (
  SELECT
    pool_address,
    max(latest_block) AS latest_block,
    bool_or(token0 IS NOT NULL AND token1 IS NOT NULL) AS has_tokens,
    bool_or(coalesce(pool_key_fee_pips, fee_pips) IS NOT NULL) AS has_fee,
    bool_or(tick_spacing IS NOT NULL) AS has_tick_spacing,
    bool_or(hooks_address IS NOT NULL) AS has_hooks,
    bool_or(liquidity IS NOT NULL AND sqrt_price_x96 IS NOT NULL AND tick IS NOT NULL) AS has_state
  FROM v4
  GROUP BY pool_address
),
tick_pools AS (
  SELECT lower(pool_address) AS pool_address, count(*) AS ticks, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1
)
SELECT
  count(*) AS v4_pools,
  count(*) FILTER (WHERE has_tokens AND has_fee AND has_tick_spacing AND has_hooks AND has_state) AS metadata_state_ready,
  count(*) FILTER (WHERE tp.ticks > 0) AS pools_with_ticks,
  coalesce(sum(tp.ticks), 0) AS ticks,
  max(vp.latest_block) AS latest_v4_block,
  max(tp.latest_tick_block) AS latest_tick_block
FROM v4_pools vp
LEFT JOIN tick_pools tp ON tp.pool_address = vp.pool_address;

WITH v4_pool_rows AS (
  SELECT
    lower(pool_address) AS pool_address,
    bool_or(token0 IS NOT NULL AND token1 IS NOT NULL) AS has_tokens,
    bool_or(coalesce(pool_key_fee_pips, fee_pips) IS NOT NULL) AS has_fee,
    bool_or(tick_spacing IS NOT NULL) AS has_tick_spacing,
    bool_or(hooks_address IS NOT NULL) AS has_hooks,
    bool_or(liquidity IS NOT NULL AND sqrt_price_x96 IS NOT NULL AND tick IS NOT NULL) AS has_state,
    max(latest_block) AS latest_block
  FROM protocol_pool_observations
  WHERE protocol = 'uniswap-v4'
    AND pool_address IS NOT NULL
  GROUP BY lower(pool_address)
),
tick_pools AS (
  SELECT lower(pool_address) AS pool_address, count(*) AS ticks
  FROM pool_ticks_current
  GROUP BY 1
)
SELECT
  count(*) FILTER (WHERE NOT (has_tokens AND has_fee AND has_tick_spacing AND has_hooks)) AS pools_missing_metadata,
  count(*) FILTER (WHERE NOT has_state) AS pools_missing_state,
  count(*) FILTER (WHERE has_state AND coalesce(tp.ticks, 0) = 0) AS state_ready_without_ticks,
  count(*) FILTER (WHERE coalesce(tp.ticks, 0) > 0) AS pools_with_ticks,
  max(latest_block) AS latest_block
FROM v4_pool_rows vp
LEFT JOIN tick_pools tp ON tp.pool_address = vp.pool_address;

WITH event_rows AS (
  SELECT
    event_type,
    count(*) FILTER (
      WHERE pool_address IS NOT NULL
        AND (token0 IS NULL OR token1 IS NULL OR tick_spacing IS NULL OR hooks_address IS NULL)
    ) AS missing_metadata,
    count(*) FILTER (
      WHERE pool_address IS NOT NULL
        AND (liquidity IS NULL OR sqrt_price_x96 IS NULL OR tick IS NULL)
    ) AS missing_state,
    count(*) AS rows,
    max(latest_block) AS latest_block
  FROM protocol_pool_observations
  WHERE protocol = 'uniswap-v4'
  GROUP BY event_type
)
SELECT *
FROM event_rows
ORDER BY missing_metadata DESC, missing_state DESC, event_type;
SQL
