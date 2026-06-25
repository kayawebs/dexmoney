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
REDIS="${REDIS_URL:-redis://127.0.0.1:6379}"
CHAIN_ID="${CHAIN_ID:-8453}"
APPLY=0
POOL=""
ALL=0

usage() {
  cat <<'EOF'
usage:
  ops/clear_stale_redis_ticks.sh <pool-address> [--apply]
  ops/clear_stale_redis_ticks.sh --all [--apply]

Purpose:
  Clear stale Redis initialized tick cache for V4 pools that Postgres tick coverage
  says are zero_ticks. This does not modify Postgres or contracts.

Examples:
  ops/clear_stale_redis_ticks.sh 0xbe518be37a79a7b7122f02f9278bc348b15e9565
  ops/clear_stale_redis_ticks.sh 0xbe518be37a79a7b7122f02f9278bc348b15e9565 --apply
  ops/clear_stale_redis_ticks.sh --all --apply
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      shift
      ;;
    --all)
      ALL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -n "$POOL" ]]; then
        echo "unexpected extra argument: $1" >&2
        usage >&2
        exit 2
      fi
      POOL="$1"
      shift
      ;;
  esac
done

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

if [[ "$ALL" -eq 0 && -z "$POOL" ]]; then
  usage >&2
  exit 2
fi

if [[ "$ALL" -eq 1 && -n "$POOL" ]]; then
  echo "choose either --all or a single pool, not both" >&2
  exit 2
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

target_file="$tmpdir/targets.txt"
redis_keys_file="$tmpdir/redis_tick_keys.txt"

if [[ "$ALL" -eq 1 ]]; then
  psql "$DB_URL" -X -q -At \
    --set=chain_id="$CHAIN_ID" <<'SQL' >"$target_file"
WITH tick_counts AS (
  SELECT lower(pool_address) AS pool, count(*) AS ticks
  FROM pool_ticks_current
  WHERE chain_id = :'chain_id'::bigint
  GROUP BY 1
)
SELECT lower(tc.pool_address) AS pool
FROM pool_tick_coverage tc
LEFT JOIN tick_counts t ON t.pool = lower(tc.pool_address)
WHERE tc.chain_id = :'chain_id'::bigint
  AND tc.variant = 'UniswapV4'
  AND tc.status = 'zero_ticks'
  AND COALESCE(t.ticks, 0) = 0
ORDER BY tc.updated_at DESC;
SQL
else
  printf '%s\n' "$POOL" | tr 'A-F' 'a-f' >"$target_file"
fi

if [[ ! -s "$target_file" ]]; then
  echo "no target pools found"
  exit 0
fi

redis-cli -u "$REDIS" --raw --scan --pattern 'ticks:index:*' >"$redis_keys_file"

echo "stale Redis tick cleanup"
echo "apply: $APPLY"
echo "targets: $(wc -l < "$target_file" | tr -d ' ')"
echo "redis_tick_indexes: $(wc -l < "$redis_keys_file" | tr -d ' ')"
echo

cleared=0
matched=0
while IFS= read -r pool; do
  [[ -z "$pool" ]] && continue
  pool_lc="$(printf '%s' "$pool" | tr 'A-F' 'a-f')"
  key="$(grep -i "ticks:index:${pool_lc}$" "$redis_keys_file" | head -n 1 || true)"
  if [[ -z "$key" ]]; then
    continue
  fi
  matched=$((matched + 1))
  count="$(redis-cli -u "$REDIS" SCARD "$key" 2>/dev/null || echo 0)"
  echo "pool=$pool_lc key=$key redis_ticks=$count"
  if [[ "$APPLY" -eq 1 ]]; then
    if [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 ]]; then
      redis-cli -u "$REDIS" --raw SMEMBERS "$key" \
        | xargs -r -n 500 redis-cli -u "$REDIS" DEL >/dev/null
    fi
    redis-cli -u "$REDIS" DEL "$key" >/dev/null
    redis-cli -u "$REDIS" SADD ticks:changed "$pool_lc" >/dev/null
    cleared=$((cleared + 1))
  fi
done <"$target_file"

echo
echo "matched_with_redis_ticks: $matched"
echo "cleared: $cleared"
if [[ "$APPLY" -eq 0 ]]; then
  echo "dry-run only. rerun with --apply to delete Redis tick entries."
fi
