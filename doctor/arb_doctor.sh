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

OPPORTUNITY_ID=""
TX_HASH=""
EXECUTOR_CALL=0
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="reports/doctor-$STAMP"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

usage() {
  cat <<'EOF'
Usage:
  doctor/arb_doctor.sh --opportunity-id <uuid> [--out <dir>] [--executor-call]
  doctor/arb_doctor.sh --tx-hash <0x...> [--out <dir>] [--executor-call]

Environment:
  POSTGRES_URL or DATABASE_URL
  REDIS_URL
  BASE_RPC_HTTP

Notes:
  --executor-call is off by default. Enable it only when hub eth_call behavior
  is part of the diagnosis.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --opportunity-id)
      OPPORTUNITY_ID="${2:-}"
      shift 2
      ;;
    --tx-hash)
      TX_HASH="${2:-}"
      shift 2
      ;;
    --out)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --executor-call)
      EXECUTOR_CALL=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$OPPORTUNITY_ID" && -z "$TX_HASH" ]]; then
  usage >&2
  exit 1
fi

psql_base() {
  psql "$DB_URL" -X -q --set=ON_ERROR_STOP=1 "$@"
}

if ! psql_base -Atc "SELECT 1" >/dev/null; then
  cat >&2 <<EOF
failed to connect database.

Current DB URL:
  $DB_URL

Set one of:
  POSTGRES_URL=postgres://user:password@127.0.0.1:5632/base_arb
  DATABASE_URL=postgres://user:password@127.0.0.1:5632/base_arb
EOF
  exit 1
fi

if [[ -z "$OPPORTUNITY_ID" ]]; then
  OPPORTUNITY_ID="$(
    psql_base -At --set=tx_hash="$TX_HASH" <<'SQL'
SELECT lower(opportunity_id::text)
FROM transactions
WHERE lower(tx_hash) = lower(:'tx_hash')
ORDER BY created_at DESC
LIMIT 1;
SQL
  )"
fi

if [[ -z "$OPPORTUNITY_ID" ]]; then
  echo "could not resolve opportunity_id from tx_hash=$TX_HASH" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/doctor-report.txt"
DB_CONTEXT="$OUT_DIR/db-context.txt"
REPLAY_TXT="$OUT_DIR/replay.txt"
REPLAY_LOG="$OUT_DIR/replay.log"
VALIDATE_TXT="$OUT_DIR/validate.txt"
VALIDATE_LOG="$OUT_DIR/validate.log"

write_header() {
  {
    echo "arb doctor report"
    echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
    echo "opportunity_id: $OPPORTUNITY_ID"
    echo "tx_hash: ${TX_HASH:-}"
    echo "executor_call: $EXECUTOR_CALL"
    echo "out_dir: $OUT_DIR"
    echo
  } >"$REPORT"
}

run_db_context() {
  {
    echo "== opportunity =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
SELECT
  id,
  created_at,
  block_number,
  strategy,
  token_in,
  amount_in,
  expected_profit,
  min_profit,
  path_json->>'name' AS path_name,
  jsonb_array_length(path_json->'steps') AS path_len,
  path_json::text ILIKE '%UniswapV4%' AS has_v4,
  path_json::text ILIKE '%BalancerV3%' AS has_balancer
FROM opportunities
WHERE id = :'opp'::uuid;
SQL

    echo
    echo "== simulations =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
SELECT
  created_at,
  success,
  COALESCE(revert_reason, '-') AS revert_reason,
  simulated_profit,
  net_simulated_profit,
  block_number,
  token_in,
  amount_in,
  expected_profit,
  min_profit
FROM simulations
WHERE opportunity_id = :'opp'::uuid
ORDER BY created_at DESC
LIMIT 20;
SQL

    echo
    echo "== transactions =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
SELECT
  created_at,
  status,
  tx_hash,
  nonce,
  gas_used,
  effective_gas_price,
  realized_profit,
  COALESCE(revert_reason, '-') AS revert_reason
FROM transactions
WHERE opportunity_id = :'opp'::uuid
ORDER BY created_at DESC
LIMIT 20;
SQL

    echo
    echo "== path steps =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
WITH steps AS (
  SELECT
    o.id,
    s.ordinality AS step_no,
    s.step
  FROM opportunities o
  CROSS JOIN LATERAL jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS s(step, ordinality)
  WHERE o.id = :'opp'::uuid
)
SELECT
  step_no,
  step->>'dex' AS dex,
  step->>'variant' AS variant,
  step->>'pool' AS pool,
  step->>'factory_address' AS factory,
  step->>'token_in' AS token_in,
  step->>'token_out' AS token_out,
  step->>'fee_bps' AS fee_bps,
  step->>'pool_key_fee_pips' AS pool_key_fee_pips,
  step->>'tick_spacing' AS tick_spacing,
  step->>'hooks_address' AS hooks,
  left(COALESCE(step->>'adapter_data', ''), 82) AS adapter_data_prefix
FROM steps
ORDER BY step_no;
SQL

    echo
    echo "== registry/state/tick coverage for path pools =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
WITH steps AS (
  SELECT
    s.ordinality AS step_no,
    lower(s.step->>'pool') AS pool
  FROM opportunities o
  CROSS JOIN LATERAL jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS s(step, ordinality)
  WHERE o.id = :'opp'::uuid
),
tick_counts AS (
  SELECT lower(pool_address) AS pool, count(*) AS ticks, max(block_number) AS latest_tick_block
  FROM pool_ticks_current
  GROUP BY 1
)
SELECT
  st.step_no,
  st.pool,
  p.dex,
  p.variant,
  p.enabled,
  p.factory_address,
  ps.block_number AS state_block,
  ps.updated_at AS state_updated_at,
  ps.source AS state_source,
  ps.sqrt_price_x96 IS NOT NULL AS has_sqrt,
  ps.liquidity IS NOT NULL AS has_liquidity,
  ps.tick,
  ps.reserve0 IS NOT NULL AS has_reserve0,
  ps.reserve1 IS NOT NULL AS has_reserve1,
  COALESCE(tc.ticks, 0) AS pg_ticks,
  tc.latest_tick_block
FROM steps st
LEFT JOIN pools p ON lower(p.pool_address) = st.pool
LEFT JOIN pool_states ps ON lower(ps.pool_address) = st.pool
LEFT JOIN tick_counts tc ON tc.pool = st.pool
ORDER BY st.step_no;
SQL

    echo
    echo "== protocol observations for singleton/vault pools =="
    psql_base --set=opp="$OPPORTUNITY_ID" <<'SQL'
\pset pager off
\pset format aligned
WITH steps AS (
  SELECT
    s.ordinality AS step_no,
    lower(s.step->>'pool') AS pool
  FROM opportunities o
  CROSS JOIN LATERAL jsonb_array_elements(o.path_json->'steps') WITH ORDINALITY AS s(step, ordinality)
  WHERE o.id = :'opp'::uuid
)
SELECT
  st.step_no,
  po.protocol,
  po.event_type,
  po.import_status,
  po.pool_uid,
  po.pool_address,
  po.dex,
  po.variant,
  po.fee_pips,
  po.tick_spacing,
  po.hooks_address,
  po.latest_block,
  po.updated_at
FROM steps st
JOIN protocol_pool_observations po
  ON lower(COALESCE(po.pool_address, '')) = st.pool
ORDER BY st.step_no, po.latest_block DESC
LIMIT 50;
SQL
  } >"$DB_CONTEXT" 2>&1
}

run_replay() {
  if ! cargo run -p base-arb-recorder --bin replay_simulations -- \
    --opportunity-id "$OPPORTUNITY_ID" \
    --out "$REPLAY_TXT" >"$REPLAY_LOG" 2>&1; then
    return 1
  fi
}

run_validate() {
  local args=(--opportunity "$OPPORTUNITY_ID")
  if [[ "$EXECUTOR_CALL" != "1" ]]; then
    args+=(--skip-executor-call)
  fi
  if ! cargo run -p base-arb-recorder --bin validate_route -- \
    "${args[@]}" >"$VALIDATE_TXT" 2>"$VALIDATE_LOG"; then
    return 1
  fi
}

extract_field() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    index($0, key ":") == 1 {
      sub("^[^:]+:[[:space:]]*", "", $0)
      print
      exit
    }
  ' "$file" 2>/dev/null || true
}

classify_verdict() {
  local replay="$1"
  local validate="$2"
  local replay_log="$3"
  local validate_log="$4"

  if grep -q "factory_check: MISMATCH" "$validate" 2>/dev/null; then
    echo "factory_or_pool_identity_mismatch"
  elif grep -qi "PoolMismatch" "$replay" "$replay_log" "$validate" "$validate_log" 2>/dev/null; then
    echo "pool_mismatch"
  elif grep -qi "InsufficientAllowance" "$replay" "$replay_log" "$validate" "$validate_log" 2>/dev/null; then
    echo "approval_config"
  elif grep -qi "InsufficientBalance" "$replay" "$replay_log" "$validate" "$validate_log" 2>/dev/null; then
    echo "capital_config"
  elif grep -qi "missing initialized ticks\\|missing ticks\\|tick_count=0" "$validate" "$validate_log" 2>/dev/null; then
    echo "missing_ticks"
  elif grep -Eq "simulation_block_delta: [1-9][0-9]*" "$replay" 2>/dev/null; then
    echo "intervening_or_state_change_suspected"
  elif grep -qi "BalancerV3" "$replay" "$validate" 2>/dev/null \
    && grep -qi "MinProfitNotMet" "$replay" "$validate" 2>/dev/null; then
    echo "balancer_model_or_adapter_mismatch_suspected"
  elif grep -qi "UniswapV4" "$replay" "$validate" 2>/dev/null \
    && grep -qi "historical_zero_min_result: Executor revert: MinProfitNotMet\\|MinProfitNotMet" "$replay" "$validate" 2>/dev/null; then
    echo "v4_model_or_adapter_mismatch_suspected"
  elif grep -q "final: opportunity_expected_profit" "$validate" 2>/dev/null; then
    echo "route_quote_completed_needs_review"
  else
    echo "unknown_needs_manual_review"
  fi
}

write_final_report() {
  local replay_status="$1"
  local validate_status="$2"
  local verdict
  verdict="$(classify_verdict "$REPLAY_TXT" "$VALIDATE_TXT" "$REPLAY_LOG" "$VALIDATE_LOG")"
  local replay_classification
  replay_classification="$(awk '/^== Summary ==/ { hit=1; next } hit && NF { print; exit }' "$REPLAY_TXT" 2>/dev/null || true)"
  local zero_min
  zero_min="$(extract_field "$REPLAY_TXT" "historical_zero_min_result")"

  {
    echo "== verdict =="
    echo "verdict: $verdict"
    echo "replay_status: $replay_status"
    echo "validate_status: $validate_status"
    echo "replay_classification: ${replay_classification:-unknown}"
    echo "zero_min_result: ${zero_min:-unknown}"
    echo
    echo "== next action =="
    case "$verdict" in
      v4_model_or_adapter_mismatch_suspected)
        echo "Focus on V4 step quote vs UniswapV4Adapter execution semantics."
        echo "Check fee_pips/pool_key_fee_pips, tick_spacing, hooks, adapter_data, and V4 delta sign/output accounting."
        ;;
      balancer_model_or_adapter_mismatch_suspected)
        echo "Focus on Balancer V3 model/rate/scaling factor and adapter calldata."
        ;;
      missing_ticks)
        echo "Repair or hydrate ticks for the affected path pools, then rerun this doctor report."
        ;;
      factory_or_pool_identity_mismatch|pool_mismatch)
        echo "Do not execute this path. Fix pool identity/factory trust/classification before reenabling."
        ;;
      approval_config)
        echo "Fix hub allowance or auto-approval path; this is not a quote model issue."
        ;;
      capital_config)
        echo "Check token_in balance and search amount config for the active executor scope."
        ;;
      route_quote_completed_needs_review)
        echo "Review Step Quote Check final profit and compare it to replay/executor output."
        ;;
      *)
        echo "Manual review required. Start from db-context.txt, replay.txt, and validate.txt."
        ;;
    esac
    echo
    echo "== artifacts =="
    echo "db_context: $DB_CONTEXT"
    echo "replay: $REPLAY_TXT"
    echo "replay_log: $REPLAY_LOG"
    echo "validate: $VALIDATE_TXT"
    echo "validate_log: $VALIDATE_LOG"
  } >>"$REPORT"
}

write_header

{
  echo "== running =="
  echo "db_context: $DB_CONTEXT"
} >>"$REPORT"
run_db_context

replay_status="ok"
{
  echo "replay: $REPLAY_TXT"
} >>"$REPORT"
if ! run_replay; then
  replay_status="failed"
fi

validate_status="ok"
{
  echo "validate: $VALIDATE_TXT"
  echo
} >>"$REPORT"
if ! run_validate; then
  validate_status="failed"
fi

write_final_report "$replay_status" "$validate_status"

cat "$REPORT"
