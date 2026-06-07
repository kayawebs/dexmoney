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
OUT_DIR="${1:-${BATCHSQL_OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/batchsql-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
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
  INTERVAL="$INTERVAL" $0
EOF
  exit 1
fi

{
  echo "batchsql report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "database: $DB_URL"
  echo
} >"$OUT_FILE"

run_query() {
  local title="$1"
  shift
  {
    echo
    echo "================================================================================"
    echo "$title"
    echo "================================================================================"
  } >>"$OUT_FILE"

  psql "$DB_URL" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --set=interval="$INTERVAL" \
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE"
}

run_query "1. table freshness" <<'SQL'
SELECT 'opportunities' AS table_name, count(*) AS n, max(created_at) AS latest
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
UNION ALL
SELECT 'simulations', count(*), max(created_at)
FROM simulations
WHERE created_at >= now() - :'interval'::interval
UNION ALL
SELECT 'transactions', count(*), max(created_at)
FROM transactions
WHERE created_at >= now() - :'interval'::interval
ORDER BY table_name;
SQL

run_query "2. simulation reason summary" <<'SQL'
SELECT
  COALESCE(
    CASE
      WHEN success THEN 'success'
      WHEN revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN revert_reason ILIKE '%net simulated profit below threshold after gas%' THEN 'below_gas'
      WHEN revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      WHEN revert_reason IS NULL OR revert_reason = '' THEN '-'
      ELSE revert_reason
    END,
    '-'
  ) AS reason_group,
  count(*) AS n,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit,
  max(net_simulated_profit::numeric) FILTER (WHERE net_simulated_profit IS NOT NULL) AS max_net_simulated_profit
FROM simulations
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC;
SQL

run_query "3. success and tx summary" <<'SQL'
SELECT
  count(*) FILTER (WHERE s.success) AS sim_success,
  count(*) FILTER (WHERE s.success AND t.tx_hash IS NOT NULL) AS success_with_tx,
  count(*) FILTER (WHERE s.success AND t.tx_hash IS NULL) AS success_without_tx,
  max(s.created_at) FILTER (WHERE s.success) AS latest_success
FROM simulations s
LEFT JOIN transactions t ON t.opportunity_id = s.opportunity_id
WHERE s.created_at >= now() - :'interval'::interval;

SELECT
  count(*) AS tx_rows,
  count(*) FILTER (WHERE status ILIKE 'Confirmed' OR status ILIKE 'Success') AS confirmed,
  count(*) FILTER (WHERE status ILIKE 'Reverted') AS reverted,
  count(*) FILTER (WHERE status ILIKE 'Pending') AS pending,
  count(*) FILTER (WHERE status ILIKE 'Dropped') AS dropped,
  max(created_at) AS latest_tx
FROM transactions
WHERE created_at >= now() - :'interval'::interval;
SQL

run_query "4. path funnel" <<'SQL'
SELECT
  s.path_name,
  count(DISTINCT o.id) AS opportunities,
  count(s.id) AS simulations,
  count(*) FILTER (WHERE s.success) AS sim_success,
  count(t.tx_hash) AS tx_rows,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%MinProfitNotMet%') AS min_profit_not_met,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%net simulated profit below threshold after gas%') AS below_gas,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%InsufficientAllowance%') AS insufficient_allowance,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%router/no-revert-data%') AS router_no_revert_data,
  max(o.created_at) AS latest_opportunity,
  max(s.created_at) AS latest_simulation,
  max(t.created_at) AS latest_tx
FROM opportunities o
LEFT JOIN simulations s ON s.opportunity_id = o.id
LEFT JOIN transactions t ON t.opportunity_id = o.id
WHERE o.created_at >= now() - :'interval'::interval
GROUP BY s.path_name
ORDER BY simulations DESC NULLS LAST, opportunities DESC
LIMIT 80;
SQL

run_query "5. latest failed simulations" <<'SQL'
SELECT
  s.created_at AS simulation_at,
  o.created_at AS opportunity_at,
  extract(epoch FROM s.created_at - o.created_at) AS sim_lag_secs,
  s.opportunity_id,
  s.id AS simulation_id,
  s.path_name,
  s.token_in,
  s.amount_in,
  s.expected_profit,
  s.min_profit,
  s.simulated_profit,
  s.gas_estimate,
  s.gas_cost_expected,
  s.gas_cost_cap,
  s.net_simulated_profit,
  s.base_fee_per_gas,
  s.max_fee_per_gas,
  s.max_priority_fee_per_gas,
  s.revert_reason,
  t.tx_hash,
  t.status AS tx_status,
  t.revert_reason AS tx_revert_reason
FROM simulations s
JOIN opportunities o ON o.id = s.opportunity_id
LEFT JOIN transactions t ON t.opportunity_id = o.id
WHERE s.created_at >= now() - :'interval'::interval
ORDER BY s.created_at DESC
LIMIT 60;
SQL

run_query "6. opportunities without simulation" <<'SQL'
WITH opp AS (
  SELECT o.*
  FROM opportunities o
  LEFT JOIN simulations s ON s.opportunity_id = o.id
  WHERE o.created_at >= now() - :'interval'::interval
    AND s.id IS NULL
)
SELECT
  count(*) AS opportunities_without_simulation,
  min(created_at) AS first_opportunity,
  max(created_at) AS latest_opportunity
FROM opp;

WITH opp AS (
  SELECT o.*
  FROM opportunities o
  LEFT JOIN simulations s ON s.opportunity_id = o.id
  WHERE o.created_at >= now() - :'interval'::interval
    AND s.id IS NULL
)
SELECT
  COALESCE(
    (
      SELECT string_agg(
        COALESCE(step->>'dex', '') || ':' ||
        COALESCE(step->>'variant', '') || ':' ||
        right(COALESCE(step->>'pool', step->>'pool_address', ''), 6),
        ' -> '
        ORDER BY ord
      )
      FROM jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS x(step, ord)
    ),
    'unknown'
  ) AS path_signature,
  count(*) AS n,
  max(o.created_at) AS latest,
  min(o.expected_profit::numeric) AS min_expected_profit,
  max(o.expected_profit::numeric) AS max_expected_profit
FROM opp o
GROUP BY 1
ORDER BY n DESC, latest DESC
LIMIT 80;
SQL

run_query "7. required executor approvals from InsufficientAllowance" <<'SQL'
WITH bad AS (
  SELECT s.opportunity_id, s.created_at, s.path_name, o.path_json
  FROM simulations s
  JOIN opportunities o ON o.id = s.opportunity_id
  WHERE s.created_at >= now() - :'interval'::interval
    AND s.revert_reason ILIKE '%InsufficientAllowance%'
),
steps AS (
  SELECT
    b.created_at,
    b.opportunity_id,
    b.path_name,
    ord AS step_no,
    COALESCE(step->>'dex', step->>'dex_kind') AS dex,
    COALESCE(step->>'variant', step->>'pool_variant') AS variant,
    lower(COALESCE(step->>'pool', step->>'pool_address')) AS pool,
    lower(COALESCE(step->>'token_in', step->>'tokenIn')) AS token_in,
    lower(COALESCE(step->>'token_out', step->>'tokenOut')) AS token_out
  FROM bad b
  CROSS JOIN LATERAL jsonb_array_elements(b.path_json->'steps') WITH ORDINALITY AS x(step, ord)
)
SELECT
  dex,
  variant,
  token_in,
  CASE
    WHEN variant ILIKE '%Pancake%' THEN 'PANCAKE_V3_ROUTER'
    WHEN variant ILIKE '%Slipstream%' THEN 'AERODROME_SLIPSTREAM_ROUTER'
    WHEN variant ILIKE '%Uniswap%' THEN 'UNISWAP_V3_ROUTER'
    WHEN variant ILIKE '%Aerodrome%' THEN 'AERODROME_ROUTER'
    ELSE 'UNKNOWN_ROUTER'
  END AS router_kind,
  count(*) AS step_hits,
  max(created_at) AS latest,
  array_agg(DISTINCT path_name) AS sample_paths
FROM steps
GROUP BY 1, 2, 3, 4
ORDER BY step_hits DESC, latest DESC;
SQL

run_query "8. pool state freshness at opportunity block" <<'SQL'
WITH sims AS (
  SELECT
    s.opportunity_id,
    s.created_at AS simulation_at,
    CASE
      WHEN s.success THEN 'success'
      WHEN s.revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN s.revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN s.revert_reason ILIKE '%net simulated profit below threshold after gas%' THEN 'below_gas'
      WHEN s.revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      ELSE COALESCE(s.revert_reason, '-')
    END AS result,
    o.created_at AS opportunity_at,
    o.block_number AS opportunity_block,
    o.path_json
  FROM simulations s
  JOIN opportunities o ON o.id = s.opportunity_id
  WHERE s.created_at >= now() - :'interval'::interval
),
steps AS (
  SELECT
    sims.*,
    ord AS step_no,
    lower(COALESCE(step->>'pool', step->>'pool_address')) AS pool
  FROM sims
  CROSS JOIN LATERAL jsonb_array_elements(sims.path_json->'steps') WITH ORDINALITY AS x(step, ord)
),
state_at_opp AS (
  SELECT DISTINCT ON (steps.opportunity_id, steps.step_no)
    steps.opportunity_id,
    steps.result,
    steps.step_no,
    steps.pool,
    steps.opportunity_at,
    steps.opportunity_block,
    ps.source,
    ps.block_number AS state_block,
    ps.updated_at AS state_updated_at
  FROM steps
  LEFT JOIN pool_states ps
    ON lower(ps.pool_address) = steps.pool
   AND ps.block_number <= steps.opportunity_block
  ORDER BY steps.opportunity_id, steps.step_no, ps.block_number DESC NULLS LAST, ps.updated_at DESC NULLS LAST
)
SELECT
  result,
  COALESCE(source, 'no_pool_state_match') AS source,
  count(*) AS step_n,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM opportunity_at - state_updated_at)) AS p50_state_age_secs_at_opp,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM opportunity_at - state_updated_at)) AS p90_state_age_secs_at_opp,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY opportunity_block - state_block) AS p50_state_lag_blocks_at_opp,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY opportunity_block - state_block) AS p90_state_lag_blocks_at_opp,
  max(opportunity_block - state_block) AS max_state_lag_blocks_at_opp
FROM state_at_opp
GROUP BY 1, 2
ORDER BY step_n DESC;
SQL

run_query "9. pool/state source freshness now" <<'SQL'
SELECT
  source,
  count(*) AS states,
  count(DISTINCT lower(pool_address)) AS pools,
  max(block_number) AS latest_block,
  max(updated_at) AS latest_updated_at
FROM pool_states
WHERE updated_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY states DESC;

WITH latest AS (
  SELECT DISTINCT ON (lower(pool_address))
    lower(pool_address) AS pool,
    source,
    block_number,
    updated_at
  FROM pool_states
  ORDER BY lower(pool_address), block_number DESC, updated_at DESC
)
SELECT
  source,
  count(*) AS latest_pool_count,
  max(block_number) AS latest_block,
  max(updated_at) AS latest_updated_at
FROM latest
GROUP BY 1
ORDER BY latest_pool_count DESC;
SQL

run_query "10. actionable notes" <<'SQL'
SELECT 'If InsufficientAllowance > 0: run ops/sync_executor_config.sh to approve enabled tokens to enabled routers on the executor contract.' AS note
UNION ALL
SELECT 'If sim_success = 0: execution-manager will not submit any tx; fix simulation failures first.'
UNION ALL
SELECT 'If MinProfitNotMet dominates: pick the latest opportunity_id from section 5 and run validate_route to compare local quote vs onchain/router quote.'
UNION ALL
SELECT 'If opportunities_without_simulation is high: check execution-manager logs and candidate-cache/structural-failure skip behavior.';
SQL

echo "wrote $OUT_FILE"
