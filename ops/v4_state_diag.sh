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

POOL="${1:-}"
OUT_DIR="${2:-${OUT_DIR:-reports}}"
CHAIN_ID="${CHAIN_ID:-8453}"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
REDIS="${REDIS_URL:-redis://127.0.0.1:6379}"
RPC_URL="${BASE_RPC_HTTP:-}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

UNISWAP_V4_SWAP_TOPIC="0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC="0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"

if [[ -z "$POOL" ]]; then
  cat >&2 <<'EOF'
usage:
  ops/v4_state_diag.sh <v4-pool-address> [out-dir]

example:
  ops/v4_state_diag.sh 0xbe518be37a79a7b7122f02f9278bc348b15e9565 /tmp
EOF
  exit 2
fi

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

mkdir -p "$OUT_DIR"
OUT_FILE="$OUT_DIR/v4-state-diag-$STAMP.txt"

hex_block() {
  printf '0x%x' "$1"
}

section() {
  {
    echo
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
  } >>"$OUT_FILE"
}

run_psql() {
  psql "$DB_URL" \
    -X \
    --set=ON_ERROR_STOP=1 \
    --set=pool="$POOL" \
    --set=chain_id="$CHAIN_ID" \
    --pset=pager=off \
    --pset=border=2 \
    "$@" >>"$OUT_FILE"
}

{
  echo "v4 state diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "pool: $POOL"
  echo "chain_id: $CHAIN_ID"
  echo "database: $DB_URL"
  echo "redis: $REDIS"
  echo "rpc_configured: $([[ -n "$RPC_URL" ]] && echo yes || echo no)"
  echo
  cat <<'EOF'
read guide:
- Redis pool state is what searcher/execution simulate against.
- protocol_pool_observations is the durable V4 PoolManager observation table.
- pool_states is the latest promoted runtime state history.
- pool_ticks_current/pool_tick_coverage are tick readiness, not live price readiness.
- If PoolManager has newer Swap logs for this pool_uid than protocol_pool_observations/latest Redis state, market-data promotion is stale.
- If no newer logs exist but Hub still zero-min fails, suspect V4 quote math or adapter semantics.
EOF
} >"$OUT_FILE"

section "0. Redis chain and pool state"
{
  echo "chain:current_block:"
  redis-cli -u "$REDIS" GET chain:current_block || true
  echo
  echo "pool index lookup:"
  POOL_LC="$(printf '%s' "$POOL" | tr 'A-F' 'a-f')"
  POOL_KEY="$(redis-cli -u "$REDIS" --raw GET "pool_index:$POOL_LC" 2>/dev/null || true)"
  if [[ -z "$POOL_KEY" ]]; then
    POOL_INDEX_KEY="$(redis-cli -u "$REDIS" --raw --scan --pattern "pool_index:*" 2>/dev/null | grep -i "$POOL_LC" | head -n 1 || true)"
    if [[ -n "$POOL_INDEX_KEY" ]]; then
      POOL_KEY="$(redis-cli -u "$REDIS" --raw GET "$POOL_INDEX_KEY" 2>/dev/null || true)"
    fi
  fi
  echo "pool_key=${POOL_KEY:-missing}"
  if [[ -n "$POOL_KEY" ]]; then
    redis-cli -u "$REDIS" --raw GET "$POOL_KEY" \
      | jq '{dex,variant,token0,token1,fee_bps,fee_pips,pool_key_fee_pips,hooks_address,tick_spacing,sqrt_price_x96,liquidity,tick,block_number,valid_through_block,updated_at}' 2>/dev/null \
      || redis-cli -u "$REDIS" --raw GET "$POOL_KEY"
  fi
  echo
  echo "redis tick index:"
  TICK_KEY="$(redis-cli -u "$REDIS" --raw --scan --pattern "ticks:index:*" 2>/dev/null | grep -i "$POOL_LC" | head -n 1 || true)"
  echo "tick_key=${TICK_KEY:-missing}"
  if [[ -n "$TICK_KEY" ]]; then
    redis-cli -u "$REDIS" SCARD "$TICK_KEY" || true
  fi
} >>"$OUT_FILE" 2>&1

section "1. Postgres V4 observation / promoted state / tick coverage"
run_psql <<'SQL'
\echo '1.1 protocol_pool_observations'
SELECT
  protocol,
  manager_address,
  pool_uid,
  pool_address,
  event_type,
  token0,
  token1,
  fee_bps,
  fee_pips,
  pool_key_fee_pips,
  tick_spacing,
  hooks_address,
  sqrt_price_x96,
  liquidity,
  tick,
  first_block,
  latest_block,
  logs_30d,
  discovery_source,
  import_status,
  import_reason,
  updated_at
FROM protocol_pool_observations
WHERE chain_id = :'chain_id'::bigint
  AND protocol = 'uniswap-v4'
  AND lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 5;

\echo
\echo '1.2 pools registry'
SELECT
  pool_address,
  dex,
  variant,
  token0,
  token1,
  fee_bps,
  tick_spacing,
  stable,
  enabled,
  source,
  factory_address,
  updated_at
FROM pools
WHERE chain_id = :'chain_id'::bigint
  AND lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 5;

\echo
\echo '1.3 latest promoted pool_states'
SELECT
  pool_address,
  dex,
  token0,
  token1,
  fee,
  sqrt_price_x96,
  liquidity,
  tick,
  block_number,
  source,
  updated_at
FROM pool_states
WHERE lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 10;

\echo
\echo '1.4 tick coverage'
SELECT
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
FROM pool_tick_coverage
WHERE chain_id = :'chain_id'::bigint
  AND lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 5;

\echo
\echo '1.5 current ticks'
SELECT
  count(*) AS ticks,
  min(tick) AS min_tick,
  max(tick) AS max_tick,
  max(block_number) AS latest_tick_block,
  max(updated_at) AS latest_tick_updated_at
FROM pool_ticks_current
WHERE chain_id = :'chain_id'::bigint
  AND lower(pool_address) = lower(:'pool');
SQL

OBS_ROW="$(
  psql "$DB_URL" -X -q -At \
    --set=pool="$POOL" \
    --set=chain_id="$CHAIN_ID" \
    -F $'\t' <<'SQL' || true
SELECT
  manager_address,
  pool_uid,
  latest_block
FROM protocol_pool_observations
WHERE chain_id = :'chain_id'::bigint
  AND protocol = 'uniswap-v4'
  AND lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 1;
SQL
)"

MANAGER="$(printf '%s' "$OBS_ROW" | awk -F '\t' '{print $1}')"
POOL_UID="$(printf '%s' "$OBS_ROW" | awk -F '\t' '{print $2}')"
OBS_LATEST="$(printf '%s' "$OBS_ROW" | awk -F '\t' '{print $3}')"

section "2. PoolManager logs after observed latest_block"
{
  echo "manager=${MANAGER:-missing}"
  echo "pool_uid=${POOL_UID:-missing}"
  echo "observed_latest_block=${OBS_LATEST:-missing}"
  echo "rpc_url_configured=$([[ -n "$RPC_URL" ]] && echo yes || echo no)"
} >>"$OUT_FILE"

if [[ -n "$RPC_URL" && -n "$MANAGER" && -n "$POOL_UID" && -n "$OBS_LATEST" ]]; then
  CURRENT_BLOCK="$(redis-cli -u "$REDIS" --raw GET chain:current_block 2>/dev/null || true)"
  if [[ -z "$CURRENT_BLOCK" || ! "$CURRENT_BLOCK" =~ ^[0-9]+$ ]]; then
    CURRENT_BLOCK="$((OBS_LATEST + 5000))"
  fi
  FROM_BLOCK="$((OBS_LATEST + 1))"
  if (( FROM_BLOCK > CURRENT_BLOCK )); then
    FROM_BLOCK="$CURRENT_BLOCK"
  fi

  FROM_HEX="$(hex_block "$FROM_BLOCK")"
  TO_HEX="$(hex_block "$CURRENT_BLOCK")"

  section "2.1 RPC eth_getLogs Swap/ModifyLiquidity for this pool_uid"
  {
    echo "from_block=$FROM_BLOCK"
    echo "to_block=$CURRENT_BLOCK"
    echo "swap_topic=$UNISWAP_V4_SWAP_TOPIC"
    echo "modify_liquidity_topic=$UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC"

    SWAP_PARAMS="$(jq -cn \
      --arg from "$FROM_HEX" \
      --arg to "$TO_HEX" \
      --arg address "$MANAGER" \
      --arg topic0 "$UNISWAP_V4_SWAP_TOPIC" \
      --arg topic1 "$POOL_UID" \
      '{fromBlock:$from,toBlock:$to,address:$address,topics:[[$topic0],[$topic1]]}')"
    MODIFY_PARAMS="$(jq -cn \
      --arg from "$FROM_HEX" \
      --arg to "$TO_HEX" \
      --arg address "$MANAGER" \
      --arg topic0 "$UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC" \
      --arg topic1 "$POOL_UID" \
      '{fromBlock:$from,toBlock:$to,address:$address,topics:[[$topic0],[$topic1]]}')"

    echo
    echo "swap_logs:"
    if SWAP_LOGS="$(cast rpc --rpc-url "$RPC_URL" eth_getLogs "$SWAP_PARAMS" 2>&1)"; then
      printf '%s\n' "$SWAP_LOGS" \
        | jq '{count:length, first_block:(.[0].blockNumber // null), latest_block:(.[-1].blockNumber // null), latest_tx:(.[-1].transactionHash // null)}' 2>/dev/null \
        || printf '%s\n' "$SWAP_LOGS"
    else
      printf '%s\n' "$SWAP_LOGS"
    fi

    echo
    echo "modify_liquidity_logs:"
    if MODIFY_LOGS="$(cast rpc --rpc-url "$RPC_URL" eth_getLogs "$MODIFY_PARAMS" 2>&1)"; then
      printf '%s\n' "$MODIFY_LOGS" \
        | jq '{count:length, first_block:(.[0].blockNumber // null), latest_block:(.[-1].blockNumber // null), latest_tx:(.[-1].transactionHash // null)}' 2>/dev/null \
        || printf '%s\n' "$MODIFY_LOGS"
    else
      printf '%s\n' "$MODIFY_LOGS"
    fi
  } >>"$OUT_FILE" 2>&1
else
  echo "skipped RPC log check because RPC/manager/pool_uid/latest_block is missing" >>"$OUT_FILE"
fi

section "3. Recent opportunities using this pool"
run_psql <<'SQL'
WITH recent AS (
  SELECT
    o.id,
    o.created_at,
    o.block_number,
    o.strategy,
    o.path_json->>'name' AS path_name,
    o.token_in,
    o.amount_in,
    o.expected_profit,
    o.min_profit
  FROM opportunities o
  WHERE o.created_at >= now() - interval '24 hours'
    AND o.path_json::text ILIKE '%' || replace(lower(:'pool'), '0x', '') || '%'
)
SELECT *
FROM recent
ORDER BY created_at DESC
LIMIT 20;
SQL

echo "$OUT_FILE"
