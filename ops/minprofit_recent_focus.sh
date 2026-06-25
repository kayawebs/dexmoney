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
OUT_DIR="${1:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/minprofit-recent-focus-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

mkdir -p "$OUT_DIR"

{
  echo "minprofit recent focus report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "database: $DB_URL"
} >"$OUT_FILE"

psql "$DB_URL" \
  -X \
  --set=ON_ERROR_STOP=1 \
  --set=interval="$INTERVAL" \
  --pset=pager=off \
  --pset=border=2 \
  >>"$OUT_FILE" <<'SQL'
\echo
\echo '================================================================================'
\echo '1. recent simulation reasons'
\echo '================================================================================'
SELECT
  COALESCE(NULLIF(revert_reason, ''), CASE WHEN success THEN 'success' ELSE 'unknown' END) AS reason,
  count(*) AS n,
  max(created_at) AS latest
FROM simulations
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC
LIMIT 20;

CREATE TEMP TABLE recent_mp AS
SELECT
  s.id AS simulation_id,
  s.opportunity_id,
  s.created_at AS simulation_at,
  s.block_number AS simulation_block,
  s.revert_reason,
  o.created_at AS opportunity_at,
  o.block_number AS opportunity_block,
  o.strategy,
  COALESCE(NULLIF(s.path_name, ''), o.path_json->>'name', '-') AS path_name,
  lower(COALESCE(s.token_in, o.token_in)) AS token_in,
  COALESCE(NULLIF(s.amount_in, ''), NULLIF(o.amount_in, '')) AS amount_in,
  COALESCE(NULLIF(s.expected_profit, ''), NULLIF(o.expected_profit, ''))::numeric AS expected_profit,
  COALESCE(NULLIF(s.min_profit, ''), NULLIF(o.min_profit, ''))::numeric AS min_profit,
  o.path_json
FROM simulations s
JOIN opportunities o ON o.id = s.opportunity_id
WHERE s.created_at >= now() - :'interval'::interval
  AND s.success = false
  AND COALESCE(s.revert_reason, '') ILIKE '%MinProfitNotMet%';

CREATE INDEX recent_mp_sim_idx ON recent_mp(simulation_id);
CREATE INDEX recent_mp_opp_idx ON recent_mp(opportunity_id);
CREATE INDEX recent_mp_path_idx ON recent_mp(path_name);

CREATE TEMP TABLE recent_steps AS
SELECT
  b.simulation_id,
  b.opportunity_id,
  x.ord::int AS step_no,
  COALESCE(x.step->>'dex', x.step->>'dex_kind', '-') AS dex,
  COALESCE(x.step->>'variant', x.step->>'pool_variant', '-') AS variant,
  lower(COALESCE(x.step->>'pool', x.step->>'pool_address')) AS pool,
  lower(COALESCE(x.step->>'token_in', x.step->>'tokenIn')) AS token_in,
  lower(COALESCE(x.step->>'token_out', x.step->>'tokenOut')) AS token_out
FROM recent_mp b
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.path_json->'steps', '[]'::jsonb)) WITH ORDINALITY AS x(step, ord);

CREATE INDEX recent_steps_sim_idx ON recent_steps(simulation_id);
CREATE INDEX recent_steps_pool_idx ON recent_steps(pool);

\echo
\echo '================================================================================'
\echo '2. MinProfitNotMet top paths'
\echo '================================================================================'
SELECT
  path_name,
  count(*) AS failures,
  count(DISTINCT opportunity_id) AS opportunities,
  max(simulation_at) AS latest,
  min(expected_profit) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit,
  min(min_profit) AS min_min_profit,
  max(min_profit) AS max_min_profit
FROM recent_mp
GROUP BY 1
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 40;

\echo
\echo '================================================================================'
\echo '3. MinProfitNotMet by protocol combo'
\echo '================================================================================'
WITH combos AS (
  SELECT
    b.simulation_id,
    b.opportunity_id,
    string_agg(s.dex || ':' || s.variant, ' -> ' ORDER BY s.step_no) AS protocol_combo,
    max(b.simulation_at) AS latest,
    max(b.expected_profit) AS expected_profit
  FROM recent_mp b
  JOIN recent_steps s ON s.simulation_id = b.simulation_id
  GROUP BY b.simulation_id, b.opportunity_id
)
SELECT
  protocol_combo,
  count(*) AS failures,
  count(DISTINCT opportunity_id) AS opportunities,
  max(latest) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit
FROM combos
GROUP BY 1
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 40;

\echo
\echo '================================================================================'
\echo '4. MinProfitNotMet top pools'
\echo '================================================================================'
WITH pool_counts AS (
  SELECT
    s.pool,
    max(s.dex) AS step_dex,
    max(s.variant) AS step_variant,
    count(*) AS step_failures,
    count(DISTINCT s.opportunity_id) AS opportunities,
    count(DISTINCT b.path_name) AS paths,
    max(b.simulation_at) AS latest,
    max(b.expected_profit) AS max_expected_profit
  FROM recent_steps s
  JOIN recent_mp b ON b.simulation_id = s.simulation_id
  GROUP BY s.pool
  ORDER BY count(*) DESC, max(b.expected_profit) DESC
  LIMIT 80
),
tick_counts AS (
  SELECT lower(pool_address) AS pool, count(*) AS pg_ticks, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1
)
SELECT
  pc.pool,
  COALESCE(p.dex, po.dex, pc.step_dex) AS dex,
  COALESCE(p.variant, po.variant, pc.step_variant) AS variant,
  COALESCE(tp.symbol, po.symbol, '-') AS symbol,
  pc.step_failures,
  pc.opportunities,
  pc.paths,
  pc.latest,
  pc.max_expected_profit,
  tc.status AS tick_status,
  COALESCE(t.pg_ticks, 0) AS pg_ticks,
  t.latest_tick_block,
  tc.block_number AS tick_coverage_block,
  tc.updated_at AS tick_coverage_at
FROM pool_counts pc
LEFT JOIN pools p ON lower(p.pool_address) = pc.pool
LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
LEFT JOIN LATERAL (
  SELECT po.dex, po.variant, po.symbol
  FROM protocol_pool_observations po
  WHERE lower(po.pool_address) = pc.pool
  ORDER BY po.updated_at DESC NULLS LAST
  LIMIT 1
) po ON true
LEFT JOIN tick_counts t ON t.pool = pc.pool
LEFT JOIN pool_tick_coverage tc ON tc.chain_id = 8453 AND lower(tc.pool_address) = pc.pool
ORDER BY pc.step_failures DESC, pc.max_expected_profit DESC
LIMIT 80;

\echo
\echo '================================================================================'
\echo '5. V4 pools in recent MinProfitNotMet paths'
\echo '================================================================================'
WITH v4 AS (
  SELECT
    s.pool,
    count(*) AS step_failures,
    count(DISTINCT s.opportunity_id) AS opportunities,
    max(b.simulation_at) AS latest,
    max(b.expected_profit) AS max_expected_profit
  FROM recent_steps s
  JOIN recent_mp b ON b.simulation_id = s.simulation_id
  WHERE s.dex = 'UniswapV4' OR s.variant = 'UniswapV4'
  GROUP BY s.pool
),
tick_counts AS (
  SELECT lower(pool_address) AS pool, count(*) AS pg_ticks, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1
)
SELECT
  v4.pool,
  v4.step_failures,
  v4.opportunities,
  v4.latest,
  v4.max_expected_profit,
  po.latest_block AS observation_latest_block,
  po.updated_at AS observation_updated_at,
  tc.status AS tick_status,
  COALESCE(t.pg_ticks, 0) AS pg_ticks,
  t.latest_tick_block,
  tc.block_number AS tick_coverage_block,
  tc.updated_at AS tick_coverage_at
FROM v4
LEFT JOIN protocol_pool_observations po ON po.protocol = 'uniswap-v4' AND lower(po.pool_address) = v4.pool
LEFT JOIN tick_counts t ON t.pool = v4.pool
LEFT JOIN pool_tick_coverage tc ON tc.chain_id = 8453 AND lower(tc.pool_address) = v4.pool
ORDER BY v4.step_failures DESC, v4.max_expected_profit DESC
LIMIT 80;

\echo
\echo '================================================================================'
\echo '6. newest MinProfitNotMet samples'
\echo '================================================================================'
SELECT
  simulation_at,
  opportunity_at,
  opportunity_block,
  simulation_block,
  simulation_id,
  opportunity_id,
  path_name,
  token_in,
  amount_in,
  expected_profit,
  min_profit
FROM recent_mp
ORDER BY simulation_at DESC
LIMIT 40;
SQL

echo "$OUT_FILE"
