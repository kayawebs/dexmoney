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

REPORT_FILE="${1:-}"
LIMIT="${LIMIT:-12}"
INTERVAL="${INTERVAL:-30 minutes}"
DRY_RUN="${DRY_RUN:-0}"
VALIDATE_ROUTE="${VALIDATE_ROUTE:-1}"
REPLAY="${REPLAY:-1}"
EXECUTOR_CALL="${EXECUTOR_CALL:-0}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-reports/minprofit-proof-$STAMP}"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

mkdir -p "$OUT_DIR"

if [[ -z "$REPORT_FILE" ]] && ! psql "$DB_URL" -X -q -Atc "SELECT 1" >/dev/null; then
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

extract_ids_from_report_commands() {
  sed -n '/11\. representative replay targets/,/12\. /p' "$REPORT_FILE" \
    | grep -oE -- '--opportunity-id [0-9a-fA-F-]{36}' \
    | awk '{print tolower($2)}'
}

extract_ids_from_report_table() {
  awk -F'|' '
    /11\. representative replay targets/ { in_section = 1; next }
    /12\. / { in_section = 0 }
    in_section && NF >= 7 {
      id = $7
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", id)
      if (id ~ /^[0-9a-fA-F-]{36}$/) print tolower(id)
    }
  ' "$REPORT_FILE"
}

extract_ids_from_summary_tsv() {
  awk -F'\t' '
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "opportunity_id") id_col = i
      }
      next
    }
    id_col > 0 && $id_col ~ /^[0-9a-fA-F-]{36}$/ {
      print tolower($id_col)
    }
  ' "$REPORT_FILE"
}

select_recent_ids_from_db() {
  local limit_sql="$LIMIT"
  if [[ "$limit_sql" == "0" ]]; then
    limit_sql="1000000"
  fi
  psql "$DB_URL" -X -q -At \
    --set=ON_ERROR_STOP=1 \
    --set=interval="$INTERVAL" \
    --set=limit="$limit_sql" <<'SQL'
WITH failures AS (
  SELECT
    o.id,
    COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
    COALESCE(NULLIF(s.expected_profit, ''), NULLIF(o.expected_profit, ''))::numeric AS expected_profit,
    COALESCE(NULLIF(s.min_profit, ''), NULLIF(o.min_profit, ''))::numeric AS min_profit,
    s.created_at AS simulation_at
  FROM simulations s
  JOIN opportunities o ON o.id = s.opportunity_id
  WHERE s.created_at >= now() - :'interval'::interval
    AND s.success = false
    AND COALESCE(s.revert_reason, '') ILIKE '%MinProfitNotMet%'
),
ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY path_name
      ORDER BY
        CASE WHEN min_profit > 0 THEN expected_profit / min_profit ELSE expected_profit END DESC,
        simulation_at DESC
    ) AS path_rank
  FROM failures
)
SELECT lower(id::text)
FROM ranked
WHERE path_rank <= 2
ORDER BY
  CASE WHEN min_profit > 0 THEN expected_profit / min_profit ELSE expected_profit END DESC,
  simulation_at DESC
LIMIT :'limit';
SQL
}

IDS=()
if [[ -n "$REPORT_FILE" ]]; then
  if [[ ! -f "$REPORT_FILE" ]]; then
    echo "report not found: $REPORT_FILE" >&2
    exit 1
  fi
  while IFS= read -r opportunity_id; do
    IDS+=("$opportunity_id")
  done < <(
    {
      extract_ids_from_summary_tsv
      extract_ids_from_report_commands
      extract_ids_from_report_table
    } | awk 'NF && !seen[$0]++'
  )
else
  while IFS= read -r opportunity_id; do
    IDS+=("$opportunity_id")
  done < <(select_recent_ids_from_db | awk 'NF && !seen[$0]++')
fi

if (( ${#IDS[@]} == 0 )); then
  cat >&2 <<EOF
no opportunity ids found.

Usage:
  $0 <minprofit-failure-diag-report>
  $0 <minprofit-proof-summary.tsv>

Or select recent DB samples:
  INTERVAL="$INTERVAL" LIMIT="$LIMIT" $0
EOF
  exit 1
fi

if [[ "$LIMIT" != "0" && ${#IDS[@]} -gt "$LIMIT" ]]; then
  LIMITED_IDS=()
  while IFS= read -r opportunity_id; do
    LIMITED_IDS+=("$opportunity_id")
  done < <(printf '%s\n' "${IDS[@]}" | head -n "$LIMIT")
  IDS=("${LIMITED_IDS[@]}")
fi

SUMMARY="$OUT_DIR/summary.tsv"
{
  echo -e "idx\topportunity_id\treplay_status\treplay_classification\tzero_min_result\tvalidate_status\tvalidate_bucket\texpected_profit\tmin_profit\tlatest_local_profit\tredis_local_profit\tredis_expected_diff_bps\tfirst_step_delta\texecutor_original\texecutor_zero_min\tdiagnosis_hint\treplay_file\tvalidate_file"
} >"$SUMMARY"

echo "minprofit proof batch"
echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
echo "report: ${REPORT_FILE:-db-selected}"
echo "interval: $INTERVAL"
echo "limit: $LIMIT"
echo "targets: ${#IDS[@]}"
echo "out_dir: $OUT_DIR"
echo "replay: $REPLAY"
echo "validate_route: $VALIDATE_ROUTE"
echo "executor_call: $EXECUTOR_CALL"
echo "dry_run: $DRY_RUN"
echo

classify_validate_output() {
  local file="$1"
  local log_file="${2:-}"
  if [[ ! -s "$file" ]]; then
    echo "missing-output"
  elif grep -q "skipped_singleton_vault" "$file" 2>/dev/null; then
    echo "singleton_vault_diagnostic_fallback"
  elif grep -q "registry pool state fetch by block hash is not implemented for singleton/vault dex UniswapV4" "$file" "$log_file" 2>/dev/null; then
    echo "diagnostic_missing_uniswap_v4_state_fetch"
  elif grep -q "registry pool state fetch by block hash is not implemented for singleton/vault dex BalancerV3" "$file" "$log_file" 2>/dev/null; then
    echo "diagnostic_missing_balancer_v3_state_fetch"
  elif grep -q "local quote not implemented for UniswapV4" "$file"; then
    echo "local_quote_missing_uniswap_v4"
  elif grep -q "local quote not implemented for BalancerV3" "$file"; then
    echo "local_quote_missing_balancer_v3"
  elif grep -q "failed local quote" "$file"; then
    echo "local_quote_failed"
  elif grep -q "failed Redis local quote" "$file"; then
    echo "redis_local_quote_failed"
  elif grep -q "onchain_state: FAILED" "$file"; then
    echo "onchain_state_failed"
  elif grep -q "factory_check: MISMATCH" "$file"; then
    echo "factory_mismatch"
  elif grep -q "factory_check: FAILED" "$file"; then
    echo "factory_check_failed"
  elif grep -Eq "executor_call(_[a-z_]+)?: FAILED" "$file"; then
    echo "executor_call_failed"
  elif grep -q "final: opportunity_expected_profit" "$file"; then
    echo "route_quote_completed"
  else
    echo "unknown"
  fi
}

extract_replay_classification() {
  local file="$1"
  awk '
    /^== Summary ==/ { in_summary = 1; next }
    in_summary && NF { print; exit }
  ' "$file" 2>/dev/null || true
}

extract_zero_min_result() {
  local file="$1"
  awk '
    /^historical_zero_min_result:/ {
      sub(/^historical_zero_min_result:[[:space:]]*/, "", $0)
      gsub(/[[:space:]]+/, " ", $0)
      print
      exit
    }
  ' "$file" 2>/dev/null || true
}

extract_route_field() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    $0 ~ "^" key ":" {
      sub("^[^:]+:[[:space:]]*", "", $0)
      print
      exit
    }
  ' "$file" 2>/dev/null || true
}

extract_final_field() {
  local file="$1"
  local line_prefix="$2"
  local key="$3"
  awk -v prefix="$line_prefix" -v key="$key" '
    index($0, prefix) == 1 {
      n = split($0, parts, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        split(parts[i], kv, "=")
        if (kv[1] == key) {
          print kv[2]
          exit
        }
      }
    }
  ' "$file" 2>/dev/null || true
}

numeric_diff_bps() {
  local a="${1:-}"
  local b="${2:-}"
  if [[ -z "$a" || -z "$b" || "$a" == "unavailable" || "$b" == "unavailable" ]]; then
    echo "-"
    return
  fi
  awk -v a="$a" -v b="$b" 'BEGIN {
    if (a + 0 == 0 && b + 0 == 0) {
      print 0
      exit
    }
    max = (a + 0 > b + 0) ? a + 0 : b + 0
    diff = (a + 0 > b + 0) ? a - b : b - a
    printf "%.2f", 10000.0 * diff / max
  }'
}

decimal_lt() {
  local a="${1:-0}"
  local b="${2:-0}"
  a="$(printf '%s' "$a" | sed 's/^0*//')"
  b="$(printf '%s' "$b" | sed 's/^0*//')"
  [[ -n "$a" ]] || a="0"
  [[ -n "$b" ]] || b="0"
  if (( ${#a} < ${#b} )); then
    return 0
  fi
  if (( ${#a} > ${#b} )); then
    return 1
  fi
  [[ "$a" < "$b" ]]
}

extract_first_step_delta() {
  local file="$1"
  awk '
    function field_value(line, key,    n, parts, i, kv) {
      n = split(line, parts, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        split(parts[i], kv, "=")
        if (kv[1] == key) return kv[2]
      }
      return ""
    }
    /^recorded step [0-9]+:/ {
      step = $3
      gsub(":", "", step)
      rec[step] = field_value($0, "amount_out")
      pool[step] = field_value($0, "pool")
      variant[step] = field_value($0, "variant")
    }
    /^redis step [0-9]+: amount_in=/ {
      step = $3
      gsub(":", "", step)
      red[step] = field_value($0, "amount_out")
    }
    END {
      for (i = 1; i <= 32; i++) {
        if (rec[i] == "" || red[i] == "") continue
        max = (rec[i] + 0 > red[i] + 0) ? rec[i] + 0 : red[i] + 0
        diff = (rec[i] + 0 > red[i] + 0) ? rec[i] - red[i] : red[i] - rec[i]
        bps = (max == 0) ? 0 : 10000.0 * diff / max
        if (bps > 1.0) {
          printf "step=%s diff_bps=%.2f recorded=%s redis=%s pool=%s variant=%s", i, bps, rec[i], red[i], pool[i], variant[i]
          exit
        }
      }
      print "-"
    }
  ' "$file" 2>/dev/null || true
}

extract_executor_call_result() {
  local file="$1"
  local label="$2"
  awk -v label="$label" '
    index($0, label ": ok") == 1 {
      print "ok"
      exit
    }
    index($0, label ": FAILED") == 1 {
      n = split($0, parts, /[[:space:]]+/)
      for (i = 1; i <= n; i++) {
        split(parts[i], kv, "=")
        if (kv[1] == "decoded") {
          gsub(":", "", kv[2])
          print kv[2]
          exit
        }
      }
      print "failed_unknown"
      exit
    }
  ' "$file" 2>/dev/null || true
}

diagnosis_hint() {
  local validate_bucket="$1"
  local zero_min_result="$2"
  local expected_profit="$3"
  local min_profit="$4"
  local latest_local_profit="$5"
  local redis_local_profit="$6"
  local first_step_delta="$7"
  local executor_original="$8"
  local executor_zero_min="$9"

  if [[ "$executor_zero_min" == "ExecutorHub.MinProfitNotMet" ]]; then
    echo "executor_zero_min_not_profitable"
    return
  fi
  if [[ "$executor_zero_min" == "ok" && "$executor_original" == "ExecutorHub.MinProfitNotMet" ]]; then
    echo "executor_profit_below_configured_min"
    return
  fi
  if [[ "$executor_zero_min" == "UniswapV4Adapter.NoOutput" || "$executor_original" == "UniswapV4Adapter.NoOutput" ]]; then
    echo "uniswap_v4_adapter_no_output"
    return
  fi
  if [[ "$executor_zero_min" != "-" && "$executor_zero_min" != "ok" && "$executor_zero_min" != "" ]]; then
    echo "executor_zero_min_failed:$executor_zero_min"
    return
  fi
  if [[ "$validate_bucket" == *"missing"* || "$validate_bucket" == *"failed"* || "$validate_bucket" == *"fallback"* ]]; then
    echo "diagnostic_or_data_gap:$validate_bucket"
    return
  fi
  if [[ "$redis_local_profit" =~ ^[0-9]+$ && "$min_profit" =~ ^[0-9]+$ ]] \
    && decimal_lt "$redis_local_profit" "$min_profit"; then
    echo "current_state_not_profitable"
    return
  fi
  if [[ "$latest_local_profit" =~ ^[0-9]+$ && "$min_profit" =~ ^[0-9]+$ ]] \
    && decimal_lt "$latest_local_profit" "$min_profit"; then
    echo "latest_onchain_state_not_profitable"
    return
  fi
  if [[ "$first_step_delta" != "-" ]]; then
    echo "recorded_vs_redis_quote_diverged"
    return
  fi
  if [[ "$zero_min_result" == *"MinProfitNotMet"* ]]; then
    echo "executor_adapter_semantics_or_historical_state"
    return
  fi
  if [[ "$expected_profit" =~ ^[0-9]+$ && "$min_profit" =~ ^[0-9]+$ ]] \
    && decimal_lt "$expected_profit" "$min_profit"; then
    echo "bad_candidate_expected_below_min"
    return
  fi
  echo "needs_single_case_doctor"
}

tsv_field() {
  printf '%s' "${1:-}" \
    | tr '\t\r\n' '   ' \
    | sed 's/[[:space:]][[:space:]]*/ /g; s/^ //; s/ $//'
}

idx=0
for opportunity_id in "${IDS[@]}"; do
  idx=$((idx + 1))
  short="${opportunity_id:0:8}"
  replay_file="$OUT_DIR/replay-$short.txt"
  validate_file="$OUT_DIR/validate-$short.txt"
  replay_status="skipped"
  validate_status="skipped"
  replay_classification="-"
  zero_min_result="-"
  validate_bucket="-"
  expected_profit="-"
  min_profit="-"
  latest_local_profit="-"
  redis_local_profit="-"
  redis_expected_diff_bps="-"
  first_step_delta="-"
  executor_original="-"
  executor_zero_min="-"
  hint="-"

  echo "[$idx/${#IDS[@]}] $opportunity_id"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo -e "$idx\t$opportunity_id\tdry_run\t-\t-\tdry_run\t-\t-\t-\t-\t-\t-\t-\t-\t-\tdry_run\t$replay_file\t$validate_file" >>"$SUMMARY"
    continue
  fi

  if [[ "$REPLAY" == "1" ]]; then
    echo "  replay -> $replay_file"
    replay_status="ok"
    if ! cargo run -p base-arb-recorder --bin replay_simulations -- \
      --opportunity-id "$opportunity_id" \
      --out "$replay_file" >"$OUT_DIR/replay-$short.log" 2>&1; then
      replay_status="failed"
    fi
    replay_classification="$(extract_replay_classification "$replay_file")"
    [[ -n "$replay_classification" ]] || replay_classification="no-summary"
    zero_min_result="$(extract_zero_min_result "$replay_file")"
    [[ -n "$zero_min_result" ]] || zero_min_result="-"
  fi

  if [[ "$VALIDATE_ROUTE" == "1" ]]; then
    echo "  validate_route -> $validate_file"
    validate_status="ok"
    validate_args=(--opportunity "$opportunity_id")
    if [[ "$EXECUTOR_CALL" != "1" ]]; then
      validate_args+=(--skip-executor-call)
    fi
    if ! cargo run -p base-arb-recorder --bin validate_route -- \
      "${validate_args[@]}" >"$validate_file" 2>"$OUT_DIR/validate-$short.log"; then
      validate_status="failed"
    fi
    validate_bucket="$(classify_validate_output "$validate_file" "$OUT_DIR/validate-$short.log")"
    expected_profit="$(extract_route_field "$validate_file" "expected_profit")"
    min_profit="$(extract_route_field "$validate_file" "min_profit")"
    latest_local_profit="$(extract_final_field "$validate_file" "final:" "latest_local_profit")"
    redis_local_profit="$(extract_final_field "$validate_file" "redis final:" "redis_local_profit")"
    redis_expected_diff_bps="$(numeric_diff_bps "$expected_profit" "$redis_local_profit")"
    first_step_delta="$(extract_first_step_delta "$validate_file")"
    executor_original="$(extract_executor_call_result "$validate_file" "executor_call_original")"
    [[ -n "$executor_original" ]] || executor_original="-"
    executor_zero_min="$(extract_executor_call_result "$validate_file" "executor_call_zero_min")"
    [[ -n "$executor_zero_min" ]] || executor_zero_min="-"
  fi

  hint="$(diagnosis_hint \
    "$validate_bucket" \
    "$zero_min_result" \
    "$expected_profit" \
    "$min_profit" \
    "$latest_local_profit" \
    "$redis_local_profit" \
    "$first_step_delta" \
    "$executor_original" \
    "$executor_zero_min")"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$(tsv_field "$idx")" \
    "$(tsv_field "$opportunity_id")" \
    "$(tsv_field "$replay_status")" \
    "$(tsv_field "$replay_classification")" \
    "$(tsv_field "$zero_min_result")" \
    "$(tsv_field "$validate_status")" \
    "$(tsv_field "$validate_bucket")" \
    "$(tsv_field "$expected_profit")" \
    "$(tsv_field "$min_profit")" \
    "$(tsv_field "$latest_local_profit")" \
    "$(tsv_field "$redis_local_profit")" \
    "$(tsv_field "$redis_expected_diff_bps")" \
    "$(tsv_field "$first_step_delta")" \
    "$(tsv_field "$executor_original")" \
    "$(tsv_field "$executor_zero_min")" \
    "$(tsv_field "$hint")" \
    "$(tsv_field "$replay_file")" \
    "$(tsv_field "$validate_file")" >>"$SUMMARY"
done

echo
echo "summary: $SUMMARY"
column -t -s $'\t' "$SUMMARY" || cat "$SUMMARY"
echo
echo "bucket counts:"
awk -F'\t' 'NR > 1 { replay[$4]++; validate[$7]++ } END {
  print "-- replay_classification --"
  for (k in replay) print replay[k] "\t" k
  print "-- validate_bucket --"
  for (k in validate) print validate[k] "\t" k
}' "$SUMMARY" | sort -rn || true
echo
echo "diagnosis hint counts:"
awk -F'\t' 'NR > 1 { hint[$16]++ } END {
  for (k in hint) print hint[k] "\t" k
}' "$SUMMARY" | sort -rn || true
