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

INTERVAL="${INTERVAL:-1 hour}"
OUT_DIR="${1:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/tick-coverage-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
REDIS="${REDIS_URL:-}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ -z "$REDIS" ]]; then
  REDIS="redis://127.0.0.1:6779"
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
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE" 2>&1 || true
}

{
  echo "tick coverage report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "database: $DB_URL"
  echo "redis: $REDIS"
} >"$OUT_FILE"

section "0. Redis hot tick cache size"
{
  echo "+ redis-cli --scan --pattern 'ticks:index:*' | wc -l"
  redis-cli -u "$REDIS" --scan --pattern 'ticks:index:*' | wc -l
  echo
  echo "+ redis-cli SCARD ticks:changed"
  redis-cli -u "$REDIS" SCARD ticks:changed
} >>"$OUT_FILE" 2>&1 || true

run_sql "1. Authoritative tick coverage by variant" <<'SQL'
WITH active AS (
  SELECT DISTINCT ON (lower(pool_address))
    lower(pool_address) AS pool,
    block_number,
    updated_at
  FROM pool_states
  WHERE updated_at >= now() - :'interval'::interval
  ORDER BY lower(pool_address), updated_at DESC
),
tick_rows AS (
  SELECT chain_id, lower(pool_address) AS pool, count(*) AS tick_rows, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1, 2
)
SELECT
  p.dex,
  p.variant,
  count(*) AS active_pools,
  count(*) FILTER (WHERE tc.status = 'ready') AS coverage_ready,
  count(*) FILTER (WHERE tc.status = 'zero_ticks') AS coverage_zero_ticks,
  count(*) FILTER (WHERE tc.status = 'refresh_failed') AS coverage_failed,
  count(*) FILTER (WHERE tc.status IS NULL) AS coverage_missing,
  count(*) FILTER (WHERE COALESCE(tr.tick_rows, 0) > 0) AS pools_with_tick_rows,
  sum(COALESCE(tr.tick_rows, 0)) AS tick_rows,
  max(active.block_number) AS latest_state_block,
  max(tr.latest_tick_block) AS latest_tick_block
FROM active
JOIN pools p ON lower(p.pool_address) = active.pool
LEFT JOIN pool_tick_coverage tc
  ON tc.chain_id = p.chain_id
 AND lower(tc.pool_address) = active.pool
LEFT JOIN tick_rows tr
  ON tr.chain_id = p.chain_id
 AND tr.pool = active.pool
WHERE p.enabled
  AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3', 'UniswapV4')
GROUP BY 1, 2
ORDER BY active_pools DESC;
SQL

run_sql "2. Actionable tick gaps" <<'SQL'
WITH active AS (
  SELECT DISTINCT ON (lower(pool_address))
    lower(pool_address) AS pool,
    block_number,
    updated_at
  FROM pool_states
  WHERE updated_at >= now() - :'interval'::interval
  ORDER BY lower(pool_address), updated_at DESC
),
tick_rows AS (
  SELECT chain_id, lower(pool_address) AS pool, count(*) AS tick_rows, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1, 2
)
SELECT
  p.pool_address,
  COALESCE(tp.symbol, p.token0 || '/' || p.token1) AS symbol,
  p.dex,
  p.variant,
  p.source,
  p.factory_address,
  active.block_number AS latest_state_block,
  active.updated_at AS latest_state_at,
  tc.status AS coverage_status,
  tc.tick_count AS coverage_tick_count,
  tc.source AS coverage_source,
  tc.from_block AS coverage_from_block,
  tc.to_block AS coverage_to_block,
  tc.updated_at AS coverage_updated_at,
  COALESCE(tr.tick_rows, 0) AS tick_rows,
  tr.latest_tick_block
FROM active
JOIN pools p ON lower(p.pool_address) = active.pool
LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
LEFT JOIN pool_tick_coverage tc
  ON tc.chain_id = p.chain_id
 AND lower(tc.pool_address) = active.pool
LEFT JOIN tick_rows tr
  ON tr.chain_id = p.chain_id
 AND tr.pool = active.pool
WHERE p.enabled
  AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3', 'UniswapV4')
  AND (
    tc.status IS NULL
    OR tc.status = 'refresh_failed'
    OR (tc.status = 'ready' AND COALESCE(tr.tick_rows, 0) = 0)
  )
ORDER BY active.updated_at DESC
LIMIT 100;
SQL

run_sql "3. V4 observation readiness" <<'SQL'
SELECT
  event_type,
  count(*) AS observations,
  count(*) FILTER (WHERE token0 IS NULL OR token1 IS NULL OR fee_pips IS NULL OR tick_spacing IS NULL OR hooks_address IS NULL) AS missing_metadata,
  count(*) FILTER (WHERE sqrt_price_x96 IS NULL OR liquidity IS NULL OR tick IS NULL) AS missing_state,
  count(*) FILTER (WHERE lower(COALESCE(hooks_address, '0x0000000000000000000000000000000000000000')) <> lower('0x0000000000000000000000000000000000000000')) AS nonzero_hook,
  count(*) FILTER (WHERE COALESCE(pool_key_fee_pips, fee_pips, 0) >= 8388608) AS dynamic_fee_flagged,
  max(latest_block) AS latest_block,
  max(updated_at) AS latest
FROM protocol_pool_observations
WHERE protocol = 'uniswap-v4'
GROUP BY 1
ORDER BY observations DESC;
SQL

run_sql "4. Recent hydration run progress" <<'SQL'
SELECT *
FROM pool_tick_hydration_runs
ORDER BY updated_at DESC
LIMIT 10;
SQL

echo "$OUT_FILE"
