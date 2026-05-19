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

DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"
RPC_URL="${BASE_RPC_HTTP:-}"
EXECUTOR="${EXECUTOR_CONTRACT:-}"
OWNER_PRIVATE_KEY="${EXECUTOR_OWNER_PRIVATE_KEY:-${DEPLOYER_PRIVATE_KEY:-}}"
MAX_UINT="115792089237316195423570985008687907853269984665640564039457584007913129639935"
ZERO_ADDRESS="0x0000000000000000000000000000000000000000"
PANCAKE_V3_ROUTER_DEFAULT="0x1b81D678ffb9C0263b24A97847620C99d213eB14"
DRY_RUN="${DRY_RUN:-0}"

require_env() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    echo "missing required env: $name" >&2
    exit 1
  fi
}

require_env "POSTGRES_URL or DATABASE_URL" "$DB_URL"
require_env "BASE_RPC_HTTP" "$RPC_URL"
require_env "EXECUTOR_CONTRACT" "$EXECUTOR"
require_env "EXECUTOR_OWNER_PRIVATE_KEY or DEPLOYER_PRIVATE_KEY" "$OWNER_PRIVATE_KEY"

psql_at() {
  psql "$DB_URL" -Atc "$1"
}

is_zero() {
  [[ -z "$1" || "${1,,}" == "$ZERO_ADDRESS" ]]
}

call_bool() {
  local signature="$1"
  local value="$2"
  cast call "$EXECUTOR" "$signature" "$value" --rpc-url "$RPC_URL" | tr -d '[:space:]'
}

send_or_print() {
  local signature="$1"
  shift
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN cast send %s "%s"' "$EXECUTOR" "$signature"
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  cast send "$EXECUTOR" "$signature" "$@" --rpc-url "$RPC_URL" --private-key "$OWNER_PRIVATE_KEY"
}

ensure_mapping() {
  local label="$1"
  local view_signature="$2"
  local set_signature="$3"
  local value="$4"

  if is_zero "$value"; then
    return 0
  fi

  local current
  current="$(call_bool "$view_signature" "$value")"
  if [[ "$current" == "true" ]]; then
    echo "skip $label $value"
    return 0
  fi

  echo "set $label $value"
  send_or_print "$set_signature" "$value" true
}

ensure_approval() {
  local token="$1"
  local spender="$2"

  if is_zero "$token" || is_zero "$spender"; then
    return 0
  fi

  local allowance
  allowance="$(cast call "$token" "allowance(address,address)(uint256)" "$EXECUTOR" "$spender" --rpc-url "$RPC_URL" | awk '{print $1}')"
  if [[ "$allowance" != "0" && -n "$allowance" ]]; then
    echo "skip approval token=$token spender=$spender allowance=$allowance"
    return 0
  fi

  echo "approve token=$token spender=$spender"
  send_or_print "approveToken(address,address,uint256)" "$token" "$spender" "$MAX_UINT"
}

mapfile -t TOKENS < <(psql_at "
  SELECT DISTINCT lower(token)
  FROM (
    SELECT token0 AS token FROM pools WHERE enabled = TRUE
    UNION
    SELECT token1 AS token FROM pools WHERE enabled = TRUE
    UNION
    SELECT token0 AS token FROM token_pairs WHERE enabled = TRUE
    UNION
    SELECT token1 AS token FROM token_pairs WHERE enabled = TRUE
  ) t
  WHERE token IS NOT NULL
    AND lower(token) <> lower('$ZERO_ADDRESS')
  ORDER BY 1;
")

mapfile -t POOLS < <(psql_at "
  SELECT DISTINCT lower(pool_address)
  FROM pools
  WHERE enabled = TRUE
    AND pool_address IS NOT NULL
    AND lower(pool_address) <> lower('$ZERO_ADDRESS')
  ORDER BY 1;
")

ROUTERS=()
if [[ -n "${AERODROME_ROUTER:-}" ]] && psql_at "SELECT EXISTS (SELECT 1 FROM pools WHERE enabled = TRUE AND dex = 'Aerodrome' AND variant = 'AerodromeVolatile');" | grep -q '^t$'; then
  ROUTERS+=("$AERODROME_ROUTER")
fi
if [[ -n "${UNISWAP_V3_ROUTER:-}" ]] && psql_at "SELECT EXISTS (SELECT 1 FROM pools WHERE enabled = TRUE AND dex = 'UniswapV3');" | grep -q '^t$'; then
  ROUTERS+=("$UNISWAP_V3_ROUTER")
fi
if psql_at "SELECT EXISTS (SELECT 1 FROM pools WHERE enabled = TRUE AND dex = 'PancakeSwap');" | grep -q '^t$'; then
  ROUTERS+=("${PANCAKE_V3_ROUTER:-$PANCAKE_V3_ROUTER_DEFAULT}")
fi

FACTORIES=()
if [[ -n "${AERODROME_POOL_FACTORY:-}" ]] && psql_at "SELECT EXISTS (SELECT 1 FROM pools WHERE enabled = TRUE AND dex = 'Aerodrome' AND variant = 'AerodromeVolatile');" | grep -q '^t$'; then
  FACTORIES+=("$AERODROME_POOL_FACTORY")
fi

echo "executor: $EXECUTOR"
echo "tokens: ${#TOKENS[@]}, pools: ${#POOLS[@]}, routers: ${#ROUTERS[@]}, factories: ${#FACTORIES[@]}"

for router in "${ROUTERS[@]}"; do
  ensure_mapping "routerWhitelist" "routerWhitelist(address)(bool)" "setRouterWhitelist(address,bool)" "$router"
done

for factory in "${FACTORIES[@]}"; do
  ensure_mapping "factoryWhitelist" "factoryWhitelist(address)(bool)" "setFactoryWhitelist(address,bool)" "$factory"
done

for pool in "${POOLS[@]}"; do
  ensure_mapping "poolWhitelist" "poolWhitelist(address)(bool)" "setPoolWhitelist(address,bool)" "$pool"
done

for token in "${TOKENS[@]}"; do
  ensure_mapping "tokenWhitelist" "tokenWhitelist(address)(bool)" "setTokenWhitelist(address,bool)" "$token"
  for router in "${ROUTERS[@]}"; do
    ensure_approval "$token" "$router"
  done
done

echo "executor config sync complete"
