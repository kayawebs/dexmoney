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
LOG_SINCE="${LOG_SINCE:-30m}"
OUT_DIR="${1:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/searcher-quality-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
REDIS="${REDIS_URL:-}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.apps.yml}"
ENV_FILE="${ENV_FILE:-.env.docker}"
DOCKER_SUDO="${DOCKER_SUDO:-1}"

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
  section "$1"
  shift
  {
    echo "+ $*"
    "$@"
  } >>"$OUT_FILE" 2>&1 || true
}

docker_compose_logs() {
  "${DOCKER_CMD[@]}" compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" logs --since "$LOG_SINCE" "$@"
}

{
  echo "searcher quality diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "log_since: $LOG_SINCE"
  echo "database: $DB_URL"
  echo "redis: $REDIS"
  echo "compose_file: $COMPOSE_FILE"
} >"$OUT_FILE"

SEARCHER_LOG="$TMP_DIR/searcher.log"
RAW_SEARCHER_LOG="$TMP_DIR/searcher.raw.log"
docker_compose_logs searcher >"$RAW_SEARCHER_LOG" 2>&1 || true
perl -pe 's/\e\[[0-9;]*m//g' "$RAW_SEARCHER_LOG" >"$SEARCHER_LOG"

section "1. searcher cycle summaries"
grep "searcher cycle summary" "$SEARCHER_LOG" >>"$OUT_FILE" || true

section "2. searcher key counters"
{
  echo "latest chain current_block from Redis:"
  redis-cli -u "$REDIS" GET chain:current_block || true
  echo
  echo "candidate queue depth:"
  redis-cli -u "$REDIS" ZCARD candidates:priority || true
  echo
  echo "changed pool queues:"
  printf "pools:changed="
  redis-cli -u "$REDIS" SCARD pools:changed || true
  printf "ticks:changed="
  redis-cli -u "$REDIS" SCARD ticks:changed || true
  echo
  echo "summary field extracts:"
  grep "searcher cycle summary" "$SEARCHER_LOG" \
    | grep -oE "latest_chain_block=[0-9]+|latest_pool_state_block=[0-9]+|quote_attempts=[0-9]+|quote_successes=[0-9]+|quote_skipped_missing_ticks=[0-9]+|quote_skipped_tick_range_exhausted=[0-9]+|quote_skipped_error=[0-9]+|price_impact_rejected=[0-9]+|quote_model_edge_rejected=[0-9]+|min_profit_rejected=[0-9]+|candidates_emitted=[0-9]+|opportunities_created=[0-9]+|best_profit_before_impact=[0-9]+|best_profit_rejected_by_impact=[0-9]+|best_profit_after_impact=[0-9]+" \
    | sort | uniq -c || true
} >>"$OUT_FILE" 2>&1

MISSING_COUNTS="$TMP_DIR/missing_tick_pools.tsv"
grep -oE "initialized tick data missing for 0x[0-9a-fA-F]{40}" "$SEARCHER_LOG" \
  | awk '{print tolower($NF)}' \
  | sort \
  | uniq -c \
  | sort -nr \
  | head -50 \
  | awk '{print $2 "\t" $1}' >"$MISSING_COUNTS" || true

section "3. MissingTicks pools from searcher logs"
if [[ -s "$MISSING_COUNTS" ]]; then
  {
    printf "%-44s %8s %12s %s\n" "pool" "samples" "redis_ticks" "redis_key"
    while IFS=$'\t' read -r pool samples; do
      suffix="${pool#0x}"
      key="$(redis-cli -u "$REDIS" --scan --pattern "ticks:index:*$suffix" | head -n 1 || true)"
      if [[ -z "$key" ]]; then
        key="ticks:index:$pool"
      fi
      ticks="$(redis-cli -u "$REDIS" SCARD "$key" 2>/dev/null || echo "ERR")"
      printf "%-44s %8s %12s %s\n" "$pool" "$samples" "$ticks" "$key"
    done <"$MISSING_COUNTS"
  } >>"$OUT_FILE"
else
  echo "No MissingTicks pool samples found in searcher logs." >>"$OUT_FILE"
fi

if [[ -s "$MISSING_COUNTS" ]]; then
  VALUES_SQL="$(awk 'BEGIN { sep="" } { printf "%s('\''%s'\'', %s)", sep, $1, $2; sep="," }' "$MISSING_COUNTS")"
  section "4. MissingTicks pool metadata and latest state"
  psql "$DB_URL" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --pset=pager=off \
    --pset=border=2 \
    >>"$OUT_FILE" 2>&1 <<SQL || true
WITH missing(pool, samples) AS (VALUES $VALUES_SQL),
latest_state AS (
  SELECT DISTINCT ON (lower(pool_address))
    lower(pool_address) AS pool,
    block_number,
    updated_at,
    source,
    sqrt_price_x96 IS NOT NULL AS has_sqrt_price,
    liquidity IS NOT NULL AS has_liquidity,
    tick IS NOT NULL AS has_tick
  FROM pool_states
  WHERE lower(pool_address) IN (SELECT pool FROM missing)
  ORDER BY lower(pool_address), updated_at DESC
)
SELECT
  m.samples,
  m.pool,
  COALESCE(tp.symbol, p.token0 || '/' || p.token1) AS symbol,
  p.dex,
  p.variant,
  p.source AS pool_source,
  p.factory_address,
  p.updated_at AS pool_updated_at,
  ls.block_number AS latest_state_block,
  ls.updated_at AS latest_state_at,
  ls.source AS latest_state_source,
  ls.has_sqrt_price,
  ls.has_liquidity,
  ls.has_tick
FROM missing m
LEFT JOIN pools p ON lower(p.pool_address) = m.pool
LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
LEFT JOIN latest_state ls ON ls.pool = m.pool
ORDER BY m.samples DESC, m.pool;
SQL
fi

section "5. price-impact rejected samples from logs"
{
  grep "top_price_impact_rejected=" "$SEARCHER_LOG" || true
  echo
  echo "compact:"
  grep "top_price_impact_rejected=" "$SEARCHER_LOG" \
    | sed -E 's/^.*top_price_impact_rejected=//' \
    | sed -E 's/ top_quote_skipped=.*$//' || true
} >>"$OUT_FILE"

run_sql "6. recent opportunity/simulation/transaction funnel" <<'SQL'
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

WITH opp AS (
  SELECT date_trunc('minute', created_at) AS minute, count(*) AS opportunities, max(created_at) AS latest
  FROM opportunities
  WHERE created_at >= now() - :'interval'::interval
  GROUP BY 1
)
SELECT * FROM opp ORDER BY minute DESC LIMIT 60;
SQL

run_sql "7. recent opportunity quality by strategy/path" <<'SQL'
SELECT
  strategy,
  count(*) AS n,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_profit_seen,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_profit_seen,
  max(expected_profit::numeric) AS max_profit_seen,
  min(min_profit::numeric) AS min_min_profit,
  max(min_profit::numeric) AS max_min_profit
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
GROUP BY strategy
ORDER BY n DESC, max_profit_seen DESC;

SELECT
  path_json->>'name' AS path_name,
  count(*) AS n,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_profit_seen,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_profit_seen,
  max(expected_profit::numeric) AS max_profit_seen,
  min(min_profit::numeric) AS min_min_profit,
  max(min_profit::numeric) AS max_min_profit,
  max(path_json->'diagnostics'->>'modes') AS modes
FROM opportunities
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY max_profit_seen DESC
LIMIT 50;
SQL

run_sql "8. recent simulation failures" <<'SQL'
SELECT
  COALESCE(NULLIF(revert_reason, ''), CASE WHEN success THEN 'success' ELSE 'unknown_failure' END) AS reason,
  count(*) AS n,
  max(created_at) AS latest,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit
FROM simulations
WHERE created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC, max_expected_profit DESC;
SQL

run_sql "9. V3/V4/Balancer pool readiness" <<'SQL'
SELECT
  variant,
  count(*) AS pools,
  max(updated_at) AS latest_pool_update
FROM pools
WHERE enabled
GROUP BY variant
ORDER BY pools DESC;

SELECT
  p.variant,
  count(DISTINCT lower(ps.pool_address)) AS state_pools,
  max(ps.block_number) AS latest_state_block,
  max(ps.updated_at) AS latest_state_at
FROM pool_states
  AS ps
LEFT JOIN pools p ON lower(p.pool_address) = lower(ps.pool_address)
WHERE ps.updated_at >= now() - :'interval'::interval
GROUP BY p.variant
ORDER BY state_pools DESC;

SELECT
  protocol,
  event_type,
  import_status,
  count(*) AS observations,
  count(*) FILTER (WHERE token0 IS NOT NULL AND token1 IS NOT NULL) AS with_tokens,
  count(*) FILTER (WHERE sqrt_price_x96 IS NOT NULL AND liquidity IS NOT NULL AND tick IS NOT NULL) AS with_state,
  max(latest_block) AS latest_block,
  max(updated_at) AS latest_updated_at
FROM protocol_pool_observations
WHERE updated_at >= now() - :'interval'::interval
GROUP BY 1, 2, 3
ORDER BY observations DESC;

SELECT
  variant,
  status,
  count(*) AS rows,
  count(DISTINCT lower(pool_address)) AS pools,
  max(updated_at) AS latest_updated_at
FROM pool_quote_coverage
WHERE updated_at >= now() - :'interval'::interval
GROUP BY 1, 2
ORDER BY rows DESC;
SQL

run_cmd "10. latest execution-manager summaries" docker_compose_logs execution-manager

echo "wrote $OUT_FILE"
