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

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

psql "$DB_URL" \
  -X \
  --set=ON_ERROR_STOP=1 \
  --pset=pager=off \
  --pset=border=2 <<'SQL'
WITH tick_rows AS (
  SELECT
    chain_id,
    lower(pool_address) AS pool,
    count(*) AS tick_count,
    max(block_number) AS block_number,
    min(block_number) AS from_block,
    max(block_number) AS to_block
  FROM pool_ticks_current
  GROUP BY 1, 2
),
pool_meta AS (
  SELECT DISTINCT ON (p.chain_id, lower(p.pool_address))
    p.chain_id,
    lower(p.pool_address) AS pool_address,
    p.dex,
    p.variant,
    CASE p.variant
      WHEN 'AerodromeSlipstream' THEN 'aerodrome-slipstream'
      WHEN 'PancakeV3' THEN 'pancake-v3'
      WHEN 'UniswapV3' THEN 'uniswap-v3'
      WHEN 'UniswapV4' THEN 'uniswap-v4'
      ELSE NULL
    END AS protocol,
    tr.tick_count,
    tr.block_number,
    tr.from_block,
    tr.to_block
  FROM tick_rows tr
  JOIN pools p
    ON p.chain_id = tr.chain_id
   AND lower(p.pool_address) = tr.pool
  WHERE p.variant IN ('AerodromeSlipstream', 'PancakeV3', 'UniswapV3', 'UniswapV4')
  ORDER BY p.chain_id, lower(p.pool_address), p.enabled DESC, p.updated_at DESC
),
upserted AS (
  INSERT INTO pool_tick_coverage (
    chain_id,
    pool_address,
    dex,
    variant,
    protocol,
    status,
    tick_count,
    block_number,
    source,
    word_radius,
    from_block,
    to_block,
    updated_at
  )
  SELECT
    chain_id,
    pool_address,
    dex,
    variant,
    protocol,
    'ready',
    tick_count,
    block_number,
    'tick_coverage_reconcile',
    NULL,
    from_block,
    to_block,
    NOW()
  FROM pool_meta
  WHERE tick_count > 0
  ON CONFLICT (chain_id, pool_address)
  DO UPDATE SET
    dex = COALESCE(EXCLUDED.dex, pool_tick_coverage.dex),
    variant = COALESCE(EXCLUDED.variant, pool_tick_coverage.variant),
    protocol = COALESCE(EXCLUDED.protocol, pool_tick_coverage.protocol),
    status = 'ready',
    tick_count = EXCLUDED.tick_count,
    block_number = GREATEST(
      COALESCE(pool_tick_coverage.block_number, 0),
      COALESCE(EXCLUDED.block_number, 0)
    ),
    source = EXCLUDED.source,
    from_block = COALESCE(pool_tick_coverage.from_block, EXCLUDED.from_block),
    to_block = GREATEST(
      COALESCE(pool_tick_coverage.to_block, 0),
      COALESCE(EXCLUDED.to_block, 0)
    ),
    updated_at = NOW()
  RETURNING variant, tick_count
)
SELECT
  variant,
  count(*) AS reconciled_pools,
  sum(tick_count) AS tick_rows
FROM upserted
GROUP BY 1
ORDER BY reconciled_pools DESC;
SQL
