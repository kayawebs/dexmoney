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
REDIS="${REDIS_URL:-}"
RPC_URL="${BASE_RPC_HTTP:-}"
CHAIN_ID="${CHAIN_ID:-8453}"
FACTORY="${UNISWAP_V2_FACTORY:-0x8909dc15e40173ff4699343b6eb8132c65e18ec6}"
CAST_BIN="${CAST_BIN:-cast}"
APPLY=0
CLEAN_REDIS=1
POOL=""
LIMIT=0

usage() {
  cat <<'EOF'
usage:
  ops/direct_v2_canonical_diag.sh [--pool <address>] [--limit <n>] [--apply] [--no-clean-redis]

Purpose:
  Verify enabled DirectV2-style pools against the trusted factory canonical
  getPair(token0, token1) result. Non-canonical pools can pass local quote but
  must be rejected by ExecutorHub PoolMismatch, so they should not stay enabled.

Defaults:
  - scans enabled pools with factory UNISWAP_V2_FACTORY
  - dry-run only unless --apply is passed
  - with --apply, disables mismatched pools in Postgres and removes their Redis
    pool state keys so searcher cannot keep using stale runtime state

Examples:
  ops/direct_v2_canonical_diag.sh --limit 100
  ops/direct_v2_canonical_diag.sh --pool 0x0a55ebff7663e364101eae168ef471068b44576c
  ops/direct_v2_canonical_diag.sh --pool 0x0a55ebff7663e364101eae168ef471068b44576c --apply
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --no-clean-redis)
      CLEAN_REDIS=0
      shift
      ;;
    --pool)
      POOL="${2:-}"
      shift 2
      ;;
    --limit)
      LIMIT="${2:-0}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unexpected argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ -z "$RPC_URL" ]]; then
  echo "BASE_RPC_HTTP is required" >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

targets="$tmpdir/direct_v2_targets.tsv"
mismatches="$tmpdir/direct_v2_mismatches.tsv"

if [[ -n "$POOL" ]]; then
  psql "$DB_URL" -X -q -At \
    --set=chain_id="$CHAIN_ID" \
    --set=pool="$POOL" <<'SQL' >"$targets"
SELECT
  lower(pool_address),
  lower(token0),
  lower(token1),
  lower(factory_address),
  COALESCE(dex, ''),
  COALESCE(variant, ''),
  COALESCE(enabled::text, 'false')
FROM pools
WHERE chain_id = :'chain_id'::bigint
  AND lower(pool_address) = lower(:'pool')
ORDER BY updated_at DESC
LIMIT 1;
SQL
else
  psql "$DB_URL" -X -q -At \
    --set=chain_id="$CHAIN_ID" \
    --set=factory="$FACTORY" \
    --set=limit="$LIMIT" <<'SQL' >"$targets"
SELECT
  lower(pool_address),
  lower(token0),
  lower(token1),
  lower(factory_address),
  COALESCE(dex, ''),
  COALESCE(variant, ''),
  COALESCE(enabled::text, 'false')
FROM pools
WHERE chain_id = :'chain_id'::bigint
  AND enabled = TRUE
  AND lower(factory_address) = lower(:'factory')
  AND variant = 'AerodromeVolatile'
ORDER BY updated_at DESC
LIMIT CASE WHEN :'limit'::int > 0 THEN :'limit'::int ELSE 2147483647 END;
SQL
fi

if [[ ! -s "$targets" ]]; then
  echo "direct-v2 canonical diagnostic"
  echo "targets: 0"
  exit 0
fi

echo "direct-v2 canonical diagnostic"
echo "apply: $APPLY"
echo "clean_redis: $CLEAN_REDIS"
echo "chain_id: $CHAIN_ID"
echo "factory: $FACTORY"
echo "targets: $(wc -l < "$targets" | tr -d ' ')"
echo
printf "%-44s %-44s %-9s %s\n" "pool" "canonical_pair" "status" "reason"

checked=0
matched=0
mismatch=0
call_failed=0

while IFS='|' read -r pool token0 token1 factory dex variant enabled; do
  checked=$((checked + 1))
  if [[ "$enabled" != "true" ]]; then
    printf "%-44s %-44s %-9s %s\n" "$pool" "-" "SKIP" "pool is not enabled"
    continue
  fi
  if [[ "${factory,,}" != "${FACTORY,,}" ]]; then
    printf "%-44s %-44s %-9s %s\n" "$pool" "-" "SKIP" "factory $factory is not target factory"
    continue
  fi
  if [[ "$dex" != "Aerodrome" || "$variant" != "AerodromeVolatile" ]]; then
    printf "%-44s %-44s %-9s %s\n" "$pool" "-" "SKIP" "dex/variant is $dex/$variant"
    continue
  fi

  expected="$("$CAST_BIN" call "$factory" "getPair(address,address)(address)" "$token0" "$token1" --rpc-url "$RPC_URL" 2>/dev/null || true)"
  expected="${expected//$'\r'/}"
  expected="${expected//$'\n'/}"
  if [[ ! "$expected" =~ ^0x[0-9a-fA-F]{40}$ ]]; then
    call_failed=$((call_failed + 1))
    printf "%-44s %-44s %-9s %s\n" "$pool" "-" "ERROR" "getPair call failed"
    continue
  fi

  if [[ "${expected,,}" == "${pool,,}" ]]; then
    matched=$((matched + 1))
    printf "%-44s %-44s %-9s %s\n" "$pool" "$expected" "OK" "canonical"
  else
    mismatch=$((mismatch + 1))
    printf "%-44s %-44s %-9s %s\n" "$pool" "$expected" "BAD" "non-canonical factory pair"
    printf "%s|%s|%s|%s|%s|%s\n" "$pool" "$token0" "$token1" "$factory" "$expected" "non-canonical factory pair" >>"$mismatches"
  fi
done <"$targets"

if [[ "$APPLY" -eq 1 && -s "$mismatches" ]]; then
  while IFS='|' read -r pool _token0 _token1 _factory expected reason; do
    psql "$DB_URL" -X -q -v ON_ERROR_STOP=1 \
      --set=pool="$pool" \
      --set=reason="$reason; expected_pair=$expected" <<'SQL'
UPDATE pools
SET enabled = FALSE,
    updated_at = NOW()
WHERE lower(pool_address) = lower(:'pool');

UPDATE observed_pools
SET import_status = CASE
      WHEN import_status = 'imported' THEN 'classified_observed_only'
      ELSE import_status
    END,
    import_reason = :'reason',
    updated_at = NOW()
WHERE lower(pool_address) = lower(:'pool');
SQL

    if [[ "$CLEAN_REDIS" -eq 1 && -n "$REDIS" ]]; then
      suffix="${pool#0x}"
      redis-cli -u "$REDIS" --raw --scan --pattern "pool_index:*$suffix" \
        | while IFS= read -r index_key; do
            state_key="$(redis-cli -u "$REDIS" --raw GET "$index_key" 2>/dev/null || true)"
            if [[ -n "$state_key" ]]; then
              redis-cli -u "$REDIS" SREM pools:index "$state_key" >/dev/null || true
              redis-cli -u "$REDIS" DEL "$state_key" >/dev/null || true
            fi
            redis-cli -u "$REDIS" DEL "$index_key" >/dev/null || true
          done
      redis-cli -u "$REDIS" SREM pools:changed "$pool" "${pool,,}" >/dev/null || true
    fi
  done <"$mismatches"
fi

echo
echo "checked: $checked"
echo "canonical: $matched"
echo "mismatches: $mismatch"
echo "call_failed: $call_failed"

if [[ "$APPLY" -eq 0 && "$mismatch" -gt 0 ]]; then
  echo "dry-run only. rerun with --apply to disable mismatched pools."
fi

if [[ "$APPLY" -eq 1 && "$mismatch" -gt 0 ]]; then
  echo "disabled mismatched pools. restart market-data and searcher before runtime verification."
fi
