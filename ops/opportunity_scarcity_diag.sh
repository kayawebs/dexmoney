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
LOG_SINCE="${LOG_SINCE:-30m}"
OUT_DIR="${1:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/opportunity-scarcity-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
REDIS="${REDIS_URL:-}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.apps.yml}"
ENV_FILE="${ENV_FILE:-.env.docker}"
DOCKER_SUDO="${DOCKER_SUDO:-1}"
HUB_ADDRESS="${HUB_ADDRESS:-${EXECUTOR_CONTRACT:-}}"
BASE_RPC="${BASE_RPC_HTTP:-}"
USDC_ADDRESS="${USDC_ADDRESS:-0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913}"
WETH_ADDRESS="${WETH_ADDRESS:-0x4200000000000000000000000000000000000006}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ -z "$REDIS" ]]; then
  REDIS="redis://127.0.0.1:6779"
fi

mkdir -p "$OUT_DIR"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

if [[ "$DOCKER_SUDO" == "0" ]]; then
  DOCKER_CMD=(docker)
else
  DOCKER_CMD=(sudo docker)
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
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE" 2>&1 || true
}

run_cmd() {
  local title="$1"
  shift
  section "$title"
  {
    echo "+ $*"
    "$@"
  } >>"$OUT_FILE" 2>&1 || true
}

docker_compose_logs() {
  "${DOCKER_CMD[@]}" compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" logs --since "$LOG_SINCE" "$@"
}

{
  echo "opportunity scarcity diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "log_since: $LOG_SINCE"
  echo "database: $DB_URL"
  echo "redis: $REDIS"
  echo "hub: ${HUB_ADDRESS:-}"
  echo "compose_file: $COMPOSE_FILE"
  echo
  cat <<'EOF'
read guide:
- changed_pools=0 or latest_pool_state_block old: market-data/discovery/input issue.
- paths or quote_attempts low while changed_pools exists: path graph / active-pool / funded-token coverage issue.
- quote_successes high but opportunities_created low: min_profit, price-impact, or model-edge filter issue.
- dynamic_multihop_rough_* high: rough quote/model/tick/protocol data issue.
- opportunities high but simulations/transactions low: execution-manager/candidate freshness issue, not searcher scarcity.
- simulations high but no transactions: simulation pass rate or submit gate issue.
EOF
} >"$OUT_FILE"

SEARCHER_RAW_LOG="$TMP_DIR/searcher.raw.log"
SEARCHER_LOG="$TMP_DIR/searcher.log"
MARKET_RAW_LOG="$TMP_DIR/market-data.raw.log"
MARKET_LOG="$TMP_DIR/market-data.log"
EXEC_RAW_LOG="$TMP_DIR/execution-manager.raw.log"
EXEC_LOG="$TMP_DIR/execution-manager.log"

docker_compose_logs searcher >"$SEARCHER_RAW_LOG" 2>&1 || true
perl -pe 's/\e\[[0-9;]*m//g' "$SEARCHER_RAW_LOG" >"$SEARCHER_LOG"
docker_compose_logs market-data >"$MARKET_RAW_LOG" 2>&1 || true
perl -pe 's/\e\[[0-9;]*m//g' "$MARKET_RAW_LOG" >"$MARKET_LOG"
docker_compose_logs execution-manager >"$EXEC_RAW_LOG" 2>&1 || true
perl -pe 's/\e\[[0-9;]*m//g' "$EXEC_RAW_LOG" >"$EXEC_LOG"

section "1. DB and Redis freshness"
{
  echo "Redis chain/current queues:"
  printf "chain:current_block="
  redis-cli -u "$REDIS" GET chain:current_block || true
  printf "pools:changed="
  redis-cli -u "$REDIS" SCARD pools:changed || true
  printf "ticks:changed="
  redis-cli -u "$REDIS" SCARD ticks:changed || true
  printf "candidates:priority="
  redis-cli -u "$REDIS" ZCARD candidates:priority || true
} >>"$OUT_FILE" 2>&1

run_sql "1b. table freshness and recent funnel" <<'SQL'
SELECT now() AS db_now, current_database() AS db, current_user AS db_user;

SELECT 'pool_states' AS table_name, count(*) AS n, max(updated_at) AS latest
FROM pool_states
WHERE updated_at >= now() - :'interval'::interval
UNION ALL
SELECT 'opportunities', count(*), max(created_at)
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

WITH opp AS (
  SELECT date_trunc('minute', created_at) AS minute, count(*) AS opportunities, max(created_at) AS latest
  FROM opportunities
  WHERE created_at >= now() - :'interval'::interval
  GROUP BY 1
)
SELECT *
FROM opp
ORDER BY minute DESC
LIMIT 60;
SQL

section "2. searcher cycle summaries"
grep "searcher cycle summary" "$SEARCHER_LOG" | tail -80 >>"$OUT_FILE" || true

section "2b. searcher summary counter aggregate"
{
  grep "searcher cycle summary" "$SEARCHER_LOG" | awk '
    BEGIN {
      split("cycles total_cycle_ms max_cycle_ms idle_cycles changed_pool_scans changed_pools path_pools total_paths paths quote_attempts quote_successes quote_skipped quote_skipped_missing_ticks quote_skipped_tick_range_exhausted quote_skipped_error price_impact_rejected quote_model_edge_rejected min_profit_rejected candidates_emitted candidates_coalesced opportunities_created dynamic_multihop_paths dynamic_multihop_anchors dynamic_multihop_changed_edges dynamic_multihop_rough_missing_v3_state dynamic_multihop_rough_v3_spot_quote_failed dynamic_multihop_rough_unsupported_pool dynamic_multihop_rough_profit_below_min dynamic_multihop_candidate_cap_hit stale_publish_rejected best_profit_before_impact best_profit_after_impact", wanted, " ");
      for (i in wanted) keep[wanted[i]]=1;
    }
    {
      for (i=1; i<=NF; i++) {
        split($i, a, "=");
        if (a[1] in keep && a[2] ~ /^[0-9]+$/) {
          sum[a[1]] += a[2];
          last[a[1]] = a[2];
          seen[a[1]] = 1;
        }
      }
    }
    END {
      printf "%-48s %18s %18s\n", "field", "sum", "latest";
      for (i in wanted) {
        field=wanted[i];
        if (seen[field]) {
          printf "%-48s %18.0f %18s\n", field, sum[field], last[field];
        }
      }
    }' || true
  echo
  echo "Top rough quote failures from latest summaries:"
  grep "searcher cycle summary" "$SEARCHER_LOG" \
    | grep -oE "top_rough_quote_failures=[^ ]+" \
    | tail -20 || true
  echo
  echo "Top quote skipped from latest summaries:"
  grep "searcher cycle summary" "$SEARCHER_LOG" \
    | grep -oE "top_quote_skipped=[^ ]+" \
    | tail -20 || true
  echo
  echo "Top min profit rejected from latest summaries:"
  grep "searcher cycle summary" "$SEARCHER_LOG" \
    | grep -oE "top_min_profit_rejected=[^ ]+" \
    | tail -20 || true
} >>"$OUT_FILE" 2>&1

section "3. market-data sealed block summaries"
grep "market-data sealed block summary" "$MARKET_LOG" | tail -80 >>"$OUT_FILE" || true

section "3b. market-data performance aggregate"
{
  grep "market-data sealed block summary" "$MARKET_LOG" | awk '
    BEGIN {
      split("block_span events changed_pools fee_refreshed_pools watermarked_pools fetch_ms apply_ms fee_ms publish_ms total_ms", wanted, " ");
      for (i in wanted) keep[wanted[i]]=1;
    }
    {
      for (i=1; i<=NF; i++) {
        split($i, a, "=");
        if (a[1] in keep && a[2] ~ /^[0-9]+$/) {
          sum[a[1]] += a[2];
          if (a[2] > max[a[1]]) max[a[1]]=a[2];
          last[a[1]]=a[2];
          seen[a[1]]=1;
        }
      }
    }
    END {
      printf "%-30s %14s %14s %14s\n", "field", "sum", "max", "latest";
      for (i in wanted) {
        field=wanted[i];
        if (seen[field]) {
          printf "%-30s %14.0f %14.0f %14s\n", field, sum[field], max[field], last[field];
        }
      }
    }' || true
} >>"$OUT_FILE" 2>&1

run_sql "4. effective funded-token configuration" <<'SQL'
WITH defaults AS (
  SELECT
    chain_id,
    lower(token_address) AS token,
    executor_scope,
    NULLIF(BTRIM(search_amounts), '') AS search_amounts,
    NULLIF(BTRIM(min_profit), '') AS min_profit,
    updated_at
  FROM token_search_defaults
),
configs AS (
  SELECT
    tp.id,
    tp.symbol,
    tp.enabled,
    lower(tp.token0) AS token0,
    lower(tp.token1) AS token1,
    NULLIF(BTRIM(tp.token0_search_amounts), '') AS token0_pair_amounts,
    NULLIF(BTRIM(tp.token1_search_amounts), '') AS token1_pair_amounts,
    NULLIF(BTRIM(tp.token0_min_profit), '') AS token0_pair_min_profit,
    NULLIF(BTRIM(tp.token1_min_profit), '') AS token1_pair_min_profit,
    COALESCE(NULLIF(BTRIM(tp.token0_search_amounts), ''), d0_two.search_amounts, d0_all.search_amounts) AS token0_two_hop_amounts,
    COALESCE(NULLIF(BTRIM(tp.token1_search_amounts), ''), d1_two.search_amounts, d1_all.search_amounts) AS token1_two_hop_amounts,
    COALESCE(NULLIF(BTRIM(tp.token0_search_amounts), ''), d0_multi.search_amounts, d0_all.search_amounts) AS token0_multihop_amounts,
    COALESCE(NULLIF(BTRIM(tp.token1_search_amounts), ''), d1_multi.search_amounts, d1_all.search_amounts) AS token1_multihop_amounts,
    COALESCE(NULLIF(BTRIM(tp.token0_min_profit), ''), d0_two.min_profit, d0_all.min_profit) AS token0_two_hop_min_profit,
    COALESCE(NULLIF(BTRIM(tp.token1_min_profit), ''), d1_two.min_profit, d1_all.min_profit) AS token1_two_hop_min_profit,
    COALESCE(NULLIF(BTRIM(tp.token0_min_profit), ''), d0_multi.min_profit, d0_all.min_profit) AS token0_multihop_min_profit,
    COALESCE(NULLIF(BTRIM(tp.token1_min_profit), ''), d1_multi.min_profit, d1_all.min_profit) AS token1_multihop_min_profit,
    tp.updated_at
  FROM token_pairs tp
  LEFT JOIN defaults d0_all ON d0_all.chain_id = tp.chain_id AND d0_all.token = lower(tp.token0) AND d0_all.executor_scope = 'all'
  LEFT JOIN defaults d0_two ON d0_two.chain_id = tp.chain_id AND d0_two.token = lower(tp.token0) AND d0_two.executor_scope = 'two_hop'
  LEFT JOIN defaults d0_multi ON d0_multi.chain_id = tp.chain_id AND d0_multi.token = lower(tp.token0) AND d0_multi.executor_scope = 'multihop'
  LEFT JOIN defaults d1_all ON d1_all.chain_id = tp.chain_id AND d1_all.token = lower(tp.token1) AND d1_all.executor_scope = 'all'
  LEFT JOIN defaults d1_two ON d1_two.chain_id = tp.chain_id AND d1_two.token = lower(tp.token1) AND d1_two.executor_scope = 'two_hop'
  LEFT JOIN defaults d1_multi ON d1_multi.chain_id = tp.chain_id AND d1_multi.token = lower(tp.token1) AND d1_multi.executor_scope = 'multihop'
  WHERE tp.enabled
)
SELECT
  executor_scope,
  token,
  search_amounts,
  min_profit,
  updated_at
FROM defaults
WHERE search_amounts IS NOT NULL OR min_profit IS NOT NULL
ORDER BY token, executor_scope;

SELECT
  count(*) AS enabled_pairs,
  count(*) FILTER (WHERE token0_two_hop_amounts IS NOT NULL OR token1_two_hop_amounts IS NOT NULL) AS two_hop_funded_pairs,
  count(*) FILTER (WHERE token0_multihop_amounts IS NOT NULL OR token1_multihop_amounts IS NOT NULL) AS multihop_funded_pairs,
  count(*) FILTER (
    WHERE token0_two_hop_amounts IS NULL AND token1_two_hop_amounts IS NULL
      AND token0_multihop_amounts IS NULL AND token1_multihop_amounts IS NULL
  ) AS enabled_without_effective_amounts
FROM configs;

SELECT
  symbol,
  token0,
  token0_pair_amounts,
  token0_two_hop_amounts,
  token0_multihop_amounts,
  token0_two_hop_min_profit,
  token0_multihop_min_profit,
  token1,
  token1_pair_amounts,
  token1_two_hop_amounts,
  token1_multihop_amounts,
  token1_two_hop_min_profit,
  token1_multihop_min_profit,
  updated_at
FROM configs
WHERE token0_two_hop_amounts IS NOT NULL
   OR token1_two_hop_amounts IS NOT NULL
   OR token0_multihop_amounts IS NOT NULL
   OR token1_multihop_amounts IS NOT NULL
ORDER BY updated_at DESC
LIMIT 80;
SQL

section "4b. hub balances vs configured maximums"
{
  echo "hub=$HUB_ADDRESS"
  echo "usdc=$USDC_ADDRESS"
  echo "weth=$WETH_ADDRESS"
  echo
  if [[ -n "$HUB_ADDRESS" && -n "$BASE_RPC" ]] && command -v cast >/dev/null 2>&1; then
    echo "USDC balance:"
    cast call "$USDC_ADDRESS" "balanceOf(address)(uint256)" "$HUB_ADDRESS" --rpc-url "$BASE_RPC" || true
    echo "WETH balance:"
    cast call "$WETH_ADDRESS" "balanceOf(address)(uint256)" "$HUB_ADDRESS" --rpc-url "$BASE_RPC" || true
  else
    echo "skip chain balances: HUB_ADDRESS/BASE_RPC_HTTP/cast not available"
  fi
} >>"$OUT_FILE" 2>&1

run_sql "5. capital/amount/min-profit funnel" <<'SQL'
SELECT
  lower(token_in) AS token_in,
  amount_in,
  min_profit,
  count(*) AS opportunities,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1, 2, 3
ORDER BY opportunities DESC, max_expected_profit DESC
LIMIT 80;

SELECT
  strategy,
  count(*) AS opportunities,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit,
  min(min_profit::numeric) AS min_min_profit,
  max(min_profit::numeric) AS max_min_profit
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY opportunities DESC, max_expected_profit DESC;

SELECT
  path_json->>'name' AS path_name,
  count(*) AS opportunities,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit,
  max(path_json->'diagnostics'->>'modes') AS modes
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY max_expected_profit DESC
LIMIT 80;
SQL

run_sql "6. enabled path and pool readiness around funded pairs" <<'SQL'
WITH latest_states AS (
  SELECT DISTINCT ON (lower(pool_address))
    lower(pool_address) AS pool_address,
    block_number,
    updated_at,
    reserve0,
    reserve1,
    sqrt_price_x96,
    liquidity,
    tick
  FROM pool_states
  ORDER BY lower(pool_address), block_number DESC, updated_at DESC
),
defaults AS (
  SELECT chain_id, lower(token_address) AS token, executor_scope, NULLIF(BTRIM(search_amounts), '') AS search_amounts
  FROM token_search_defaults
),
pair_config AS (
  SELECT
    tp.id,
    tp.symbol,
    lower(tp.token0) AS token0,
    lower(tp.token1) AS token1,
    COALESCE(NULLIF(BTRIM(tp.token0_search_amounts), ''), d0_two.search_amounts, d0_multi.search_amounts, d0_all.search_amounts) AS token0_effective_amounts,
    COALESCE(NULLIF(BTRIM(tp.token1_search_amounts), ''), d1_two.search_amounts, d1_multi.search_amounts, d1_all.search_amounts) AS token1_effective_amounts
  FROM token_pairs tp
  LEFT JOIN defaults d0_all ON d0_all.chain_id = tp.chain_id AND d0_all.token = lower(tp.token0) AND d0_all.executor_scope = 'all'
  LEFT JOIN defaults d0_two ON d0_two.chain_id = tp.chain_id AND d0_two.token = lower(tp.token0) AND d0_two.executor_scope = 'two_hop'
  LEFT JOIN defaults d0_multi ON d0_multi.chain_id = tp.chain_id AND d0_multi.token = lower(tp.token0) AND d0_multi.executor_scope = 'multihop'
  LEFT JOIN defaults d1_all ON d1_all.chain_id = tp.chain_id AND d1_all.token = lower(tp.token1) AND d1_all.executor_scope = 'all'
  LEFT JOIN defaults d1_two ON d1_two.chain_id = tp.chain_id AND d1_two.token = lower(tp.token1) AND d1_two.executor_scope = 'two_hop'
  LEFT JOIN defaults d1_multi ON d1_multi.chain_id = tp.chain_id AND d1_multi.token = lower(tp.token1) AND d1_multi.executor_scope = 'multihop'
  WHERE tp.enabled
),
pool_readiness AS (
  SELECT
    p.token_pair_id,
    count(*) FILTER (WHERE p.enabled) AS enabled_pools,
    count(*) FILTER (
      WHERE p.enabled
        AND ls.pool_address IS NOT NULL
        AND (
          (ls.reserve0 IS NOT NULL AND ls.reserve1 IS NOT NULL)
          OR (ls.sqrt_price_x96 IS NOT NULL AND ls.liquidity IS NOT NULL AND ls.tick IS NOT NULL)
        )
    ) AS ready_state_pools,
    count(*) FILTER (WHERE p.enabled AND p.variant IN ('UniswapV3', 'PancakeV3', 'AerodromeSlipstream', 'UniswapV4')) AS v3_style_pools,
    count(*) FILTER (WHERE p.enabled AND p.variant = 'BalancerV3') AS balancer_v3_pools,
    max(ls.updated_at) AS latest_state
  FROM pools p
  LEFT JOIN latest_states ls ON ls.pool_address = lower(p.pool_address)
  GROUP BY p.token_pair_id
)
SELECT
  pc.symbol,
  pc.token0_effective_amounts,
  pc.token1_effective_amounts,
  pr.enabled_pools,
  pr.ready_state_pools,
  GREATEST(COALESCE(pr.ready_state_pools, 0) * (COALESCE(pr.ready_state_pools, 0) - 1), 0) AS ordered_two_pool_paths,
  pr.v3_style_pools,
  pr.balancer_v3_pools,
  pr.latest_state
FROM pair_config pc
LEFT JOIN pool_readiness pr ON pr.token_pair_id = pc.id
WHERE pc.token0_effective_amounts IS NOT NULL
   OR pc.token1_effective_amounts IS NOT NULL
ORDER BY ordered_two_pool_paths DESC, ready_state_pools DESC, pc.symbol
LIMIT 120;

SELECT
  p.dex,
  p.variant,
  p.source,
  count(*) AS enabled_pools,
  max(p.updated_at) AS latest_pool_update,
  max(ls.updated_at) AS latest_state
FROM pools p
LEFT JOIN latest_states ls ON ls.pool_address = lower(p.pool_address)
WHERE p.enabled
GROUP BY 1, 2, 3
ORDER BY enabled_pools DESC
LIMIT 80;
SQL

run_sql "7. V3/V4/Balancer data coverage" <<'SQL'
WITH tick_rows AS (
  SELECT chain_id, lower(pool_address) AS pool, count(*) AS tick_rows, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1, 2
),
enabled AS (
  SELECT chain_id, lower(pool_address) AS pool, dex, variant, enabled
  FROM pools
  WHERE enabled
)
SELECT
  e.dex,
  e.variant,
  count(*) AS enabled_pools,
  count(*) FILTER (WHERE tc.status = 'ready') AS coverage_ready,
  count(*) FILTER (WHERE tc.status = 'zero_ticks') AS coverage_zero_ticks,
  count(*) FILTER (WHERE tc.status = 'refresh_failed') AS coverage_refresh_failed,
  count(*) FILTER (WHERE tc.status IS NULL) AS coverage_unscanned,
  count(*) FILTER (WHERE COALESCE(tr.tick_rows, 0) > 0) AS pools_with_ticks,
  sum(COALESCE(tr.tick_rows, 0)) AS tick_rows,
  max(tr.latest_tick_block) AS latest_tick_block
FROM enabled e
LEFT JOIN pool_tick_coverage tc ON tc.chain_id = e.chain_id AND lower(tc.pool_address) = e.pool
LEFT JOIN tick_rows tr ON tr.chain_id = e.chain_id AND tr.pool = e.pool
WHERE e.variant IN ('UniswapV3', 'PancakeV3', 'AerodromeSlipstream', 'UniswapV4', 'BalancerV3')
GROUP BY 1, 2
ORDER BY enabled_pools DESC;

SELECT
  protocol,
  event_type,
  import_status,
  count(*) AS observations,
  count(*) FILTER (WHERE token0 IS NOT NULL AND token1 IS NOT NULL) AS with_tokens,
  count(*) FILTER (WHERE sqrt_price_x96 IS NOT NULL AND liquidity IS NOT NULL AND tick IS NOT NULL) AS with_v3_state,
  max(latest_block) AS latest_block,
  max(updated_at) AS latest_updated_at
FROM protocol_pool_observations
WHERE updated_at >= now() - :'interval'::interval
GROUP BY 1, 2, 3
ORDER BY observations DESC;
SQL

run_sql "8. simulation and submission outcome check" <<'SQL'
SELECT
  CASE
    WHEN success THEN 'success'
    WHEN revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
    WHEN revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
    WHEN revert_reason ILIKE '%InsufficientBalance%' THEN 'InsufficientBalance'
    WHEN revert_reason ILIKE '%PoolMismatch%' THEN 'PoolMismatch'
    WHEN revert_reason ILIKE '%trusted factory%' OR revert_reason ILIKE '%factory is not configured%' THEN 'untrusted_factory'
    WHEN revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
    WHEN revert_reason ILIKE '%0x5a7cfa65%' THEN 'UniswapV4Adapter.NoOutput'
    ELSE COALESCE(NULLIF(revert_reason, ''), 'unknown_failure')
  END AS reason,
  count(*) AS simulations,
  max(created_at) AS latest,
  count(t.id) FILTER (WHERE t.tx_hash IS NOT NULL AND t.tx_hash <> '') AS submitted_txs,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit
FROM simulations s
LEFT JOIN transactions t ON t.simulation_id = s.id
WHERE s.created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY simulations DESC
LIMIT 50;
SQL

section "9. execution-manager candidate and submit summaries"
{
  grep -E "candidate queue drain summary|execution candidate batch summary|tx submitted|unchecked tx submitted|simulation success/fail|candidate skipped because submission lock" "$EXEC_LOG" | tail -120 || true
} >>"$OUT_FILE" 2>&1

run_cmd "10. docker service status" "${DOCKER_CMD[@]}" compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" ps

echo "wrote $OUT_FILE"
