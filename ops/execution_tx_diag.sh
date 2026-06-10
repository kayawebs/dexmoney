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

TX_HASH="${1:-}"
OPPORTUNITY_ID="${2:-}"
INTERVAL="${INTERVAL:-2 hours}"
OUT_DIR="${OUT_DIR:-reports}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/execution-tx-diag-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
RPC_URL="${BASE_RPC_HTTP:-}"

if [[ -z "$TX_HASH" && -z "$OPPORTUNITY_ID" ]]; then
  cat >&2 <<EOF
usage:
  $0 <tx_hash> [opportunity_id]
  $0 - <opportunity_id>

optional env:
  INTERVAL="2 hours"
  OUT_DIR=reports
EOF
  exit 1
fi

if [[ "$TX_HASH" == "-" ]]; then
  TX_HASH=""
fi

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
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

run_cmd() {
  section "$1"
  shift
  {
    echo "+ $*"
    "$@"
  } >>"$OUT_FILE" 2>&1 || true
}

run_sql() {
  local title="$1"
  shift
  section "$title"
  psql "$DB_URL" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --set=interval="$INTERVAL" \
    --set=tx_hash="$TX_HASH" \
    --set=opportunity_id="$OPPORTUNITY_ID" \
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE" 2>&1 || true
}

{
  echo "execution tx diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "tx_hash: ${TX_HASH:-<none>}"
  echo "opportunity_id: ${OPPORTUNITY_ID:-<none>}"
  echo "interval: $INTERVAL"
  echo "database: $DB_URL"
  echo "rpc: ${RPC_URL:-<none>}"
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
SELECT 'transactions', count(*), max(created_at)
FROM transactions
WHERE created_at >= now() - :'interval'::interval
ORDER BY table_name;
SQL

run_sql "1. target transaction, simulation, opportunity" <<'SQL'
WITH target AS (
  SELECT DISTINCT
    COALESCE(t.opportunity_id, NULLIF(:'opportunity_id', '')::uuid) AS opportunity_id,
    t.simulation_id,
    t.tx_hash
  FROM transactions t
  WHERE (:'tx_hash' <> '' AND lower(t.tx_hash) = lower(:'tx_hash'))
     OR (:'opportunity_id' <> '' AND t.opportunity_id = :'opportunity_id'::uuid)
  UNION
  SELECT DISTINCT
    o.id AS opportunity_id,
    s.id AS simulation_id,
    NULL::text AS tx_hash
  FROM opportunities o
  LEFT JOIN simulations s ON s.opportunity_id = o.id
  WHERE :'opportunity_id' <> '' AND o.id = :'opportunity_id'::uuid
)
SELECT
  o.id AS opportunity_id,
  o.created_at AS opportunity_at,
  o.block_number AS opportunity_block,
  s.created_at AS simulation_at,
  EXTRACT(EPOCH FROM (s.created_at - o.created_at)) AS sim_lag_secs,
  t.created_at AS tx_recorded_at,
  EXTRACT(EPOCH FROM (t.created_at - s.created_at)) AS send_lag_secs,
  (
    SELECT string_agg(
      COALESCE(step->>'dex', '') || ':' ||
      COALESCE(step->>'variant', '') || ':' ||
      right(COALESCE(step->>'pool', step->>'pool_address', ''), 6),
      ' -> '
      ORDER BY ord
    )
    FROM jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS x(step, ord)
  ) AS path_signature,
  o.token_in,
  o.amount_in,
  o.expected_profit,
  o.min_profit,
  s.success AS sim_success,
  s.simulated_profit,
  s.gas_estimate,
  s.gas_cost_expected,
  s.net_simulated_profit,
  s.revert_reason AS sim_revert_reason,
  t.eoa,
  t.tx_hash,
  t.nonce,
  t.status AS tx_status,
  t.gas_used,
  t.effective_gas_price,
  t.realized_profit,
  t.revert_reason AS tx_revert_reason
FROM target x
LEFT JOIN opportunities o ON o.id = x.opportunity_id
LEFT JOIN simulations s ON s.id = x.simulation_id OR s.opportunity_id = o.id
LEFT JOIN transactions t ON t.opportunity_id = o.id
WHERE o.id IS NOT NULL OR t.tx_hash IS NOT NULL
ORDER BY s.created_at DESC NULLS LAST, t.created_at DESC NULLS LAST;
SQL

run_sql "2. target path steps" <<'SQL'
WITH target_opp AS (
  SELECT DISTINCT o.*
  FROM opportunities o
  LEFT JOIN transactions t ON t.opportunity_id = o.id
  WHERE (:'tx_hash' <> '' AND lower(t.tx_hash) = lower(:'tx_hash'))
     OR (:'opportunity_id' <> '' AND o.id = :'opportunity_id'::uuid)
)
SELECT
  o.id AS opportunity_id,
  x.ord AS step_no,
  x.step->>'dex' AS dex,
  x.step->>'variant' AS variant,
  COALESCE(x.step->>'pool', x.step->>'pool_address') AS pool,
  x.step->>'factory' AS factory,
  x.step->>'factory_address' AS factory_address,
  x.step->>'token_in' AS token_in,
  x.step->>'token_out' AS token_out,
  x.step->>'fee_bps' AS fee_bps,
  x.step->>'fee_pips' AS fee_pips,
  x.step->>'stable' AS stable,
  x.step->>'tick_spacing' AS tick_spacing,
  x.step AS raw_step
FROM target_opp o
CROSS JOIN LATERAL jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS x(step, ord)
ORDER BY o.created_at DESC, x.ord;
SQL

run_sql "3. recent attempts and blockers" <<'SQL'
SELECT
  t.created_at,
  t.opportunity_id,
  t.simulation_id,
  t.eoa,
  t.tx_hash,
  t.nonce,
  t.status,
  t.gas_used,
  t.effective_gas_price,
  t.realized_profit,
  t.revert_reason
FROM transactions t
WHERE t.created_at >= now() - :'interval'::interval
ORDER BY t.created_at DESC
LIMIT 100;

SELECT
  COALESCE(
    CASE
      WHEN s.success THEN 'success'
      WHEN s.revert_reason ILIKE '%MinProfitNotMet%' THEN 'MinProfitNotMet'
      WHEN s.revert_reason ILIKE '%InsufficientAllowance%' THEN 'InsufficientAllowance'
      WHEN s.revert_reason ILIKE '%net simulated profit below threshold after gas%' THEN 'below_gas'
      WHEN s.revert_reason ILIKE '%router/no-revert-data%' THEN 'router/no-revert-data'
      WHEN s.revert_reason ILIKE '%lazy approval preflight%' THEN 'lazy_approval_preflight'
      WHEN s.revert_reason IS NULL OR s.revert_reason = '' THEN '-'
      ELSE s.revert_reason
    END,
    '-'
  ) AS reason_group,
  count(*) AS n,
  max(s.created_at) AS latest,
  min(s.expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY s.expected_profit::numeric) AS p50_expected_profit,
  max(s.expected_profit::numeric) AS max_expected_profit,
  max(s.net_simulated_profit::numeric) FILTER (WHERE s.net_simulated_profit IS NOT NULL) AS max_net_simulated_profit
FROM simulations s
WHERE s.created_at >= now() - :'interval'::interval
GROUP BY 1
ORDER BY n DESC;
SQL

run_sql "4. opportunity to simulation funnel" <<'SQL'
WITH recent_opp AS (
  SELECT o.*
  FROM opportunities o
  WHERE o.created_at >= now() - :'interval'::interval
),
by_path AS (
  SELECT
    (
      SELECT string_agg(
        COALESCE(step->>'dex', '') || ':' ||
        COALESCE(step->>'variant', '') || ':' ||
        right(COALESCE(step->>'pool', step->>'pool_address', ''), 6),
        ' -> '
        ORDER BY ord
      )
      FROM jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS x(step, ord)
    ) AS path_signature,
    o.*
  FROM recent_opp o
)
SELECT
  path_signature,
  count(*) AS opportunities,
  count(s.id) AS simulations,
  count(t.tx_hash) AS txs,
  count(*) FILTER (WHERE s.success) AS sim_success,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%MinProfitNotMet%') AS min_profit_not_met,
  count(*) FILTER (WHERE s.revert_reason ILIKE '%router/no-revert-data%') AS router_no_revert_data,
  min(expected_profit::numeric) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit::numeric) AS p50_expected_profit,
  max(expected_profit::numeric) AS max_expected_profit,
  max(created_at) AS latest_opportunity
FROM by_path o
LEFT JOIN simulations s ON s.opportunity_id = o.id
LEFT JOIN transactions t ON t.opportunity_id = o.id
GROUP BY 1
ORDER BY latest_opportunity DESC, opportunities DESC
LIMIT 80;
SQL

run_sql "5. current EOA lane rows from transactions" <<'SQL'
SELECT
  eoa,
  count(*) AS tx_rows,
  max(created_at) AS latest,
  max(nonce) AS max_nonce,
  count(*) FILTER (WHERE status = 'Pending') AS pending_rows,
  count(*) FILTER (WHERE status = 'Confirmed') AS confirmed_rows,
  count(*) FILTER (WHERE status = 'Reverted') AS reverted_rows,
  count(*) FILTER (WHERE status = 'Dropped') AS dropped_rows
FROM transactions
WHERE created_at >= now() - :'interval'::interval
GROUP BY eoa
ORDER BY latest DESC;
SQL

if [[ -n "$RPC_URL" && -n "$TX_HASH" ]]; then
  run_cmd "6. chain transaction" cast tx "$TX_HASH" --rpc-url "$RPC_URL"
  run_cmd "7. chain receipt" cast receipt "$TX_HASH" --rpc-url "$RPC_URL"
  run_cmd "8. chain trace/cast run" cast run "$TX_HASH" --rpc-url "$RPC_URL"
fi

if [[ -n "${REDIS_URL:-}" ]]; then
  run_cmd "9. redis candidate queue length" redis-cli -u "$REDIS_URL" ZCARD candidates:priority
  run_cmd "10. redis top candidates" redis-cli -u "$REDIS_URL" ZREVRANGE candidates:priority 0 20 WITHSCORES
  run_cmd "11. redis failure key count" bash -lc "redis-cli -u '$REDIS_URL' --scan --pattern 'failures:*' | wc -l"
  run_cmd "12. redis EOA lane states" bash -lc "redis-cli -u '$REDIS_URL' --scan --pattern 'eoa:*:state' | sort | while read -r k; do echo \"\$k\"; redis-cli -u '$REDIS_URL' GET \"\$k\"; done"
fi

run_cmd "13. execution-manager recent logs" bash -lc "sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs --since '$INTERVAL' execution-manager | grep -E 'execution worker selected for candidate batch|tx submitted|simulation success/fail|MinProfitNotMet|router/no-revert-data|candidate skipped|candidate expired|circuit breaker|in-flight|confirmed|reverted|executor approval|worker funding|Error|WARN' | tail -500"

echo "$OUT_FILE"
