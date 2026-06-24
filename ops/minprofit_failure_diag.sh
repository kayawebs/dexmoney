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

INTERVAL="${INTERVAL:-2 hours}"
OUT_DIR="${1:-${OUT_DIR:-reports}}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_FILE="$OUT_DIR/minprofit-failure-diag-$STAMP.txt"
DB_URL="${POSTGRES_URL:-${DATABASE_URL:-}}"

if [[ -z "$DB_URL" ]]; then
  DB_URL="postgres://user:password@127.0.0.1:5632/base_arb"
fi

mkdir -p "$OUT_DIR"

if ! psql "$DB_URL" -X -q -Atc "SELECT 1" >/dev/null; then
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
  echo "minprofit failure diagnostic report"
  echo "generated_at_utc: $(date -u '+%Y-%m-%d %H:%M:%S')"
  echo "interval: $INTERVAL"
  echo "database: $DB_URL"
  echo
  cat <<'EOF'
read guide:
- If failures concentrate in one protocol combo/path: suspect quote model, adapter, or route encoding for that protocol.
- If failures have high opportunity_to_simulation lag: suspect stale candidate or execution-manager throughput.
- If opportunity block replay fails too: suspect local quote/model/calldata bug.
- If opportunity block replay succeeds but simulation/latest fails: suspect state movement or latency.
- If expected/min_profit ratio is close to 1: margin is thin; if ratio is high and still fails, suspect correctness.
- Use section 11 opportunity ids with replay_simulations for second-stage proof.
EOF
} >"$OUT_FILE"

psql "$DB_URL" \
  -X \
  --set=ON_ERROR_STOP=1 \
  --set=interval="$INTERVAL" \
  --pset=pager=off \
  --pset=border=2 \
  >>"$OUT_FILE" <<'SQL'
\echo
\echo '================================================================================'
\echo '0. database freshness'
\echo '================================================================================'
SELECT now() AS db_now, current_database() AS db, current_user AS db_user;

\echo
\echo '================================================================================'
\echo '1. simulation funnel by window'
\echo '================================================================================'
WITH windows(label, span) AS (
  VALUES
    ('30m', interval '30 minutes'),
    ('2h', interval '2 hours'),
    ('12h', interval '12 hours')
),
funnel AS (
  SELECT
    w.label,
    w.span,
    (SELECT count(*) FROM opportunities o WHERE o.created_at >= now() - w.span) AS opportunities,
    (SELECT count(*) FROM simulations s WHERE s.created_at >= now() - w.span) AS simulations,
    (SELECT count(*) FROM simulations s WHERE s.created_at >= now() - w.span AND s.success) AS simulation_success,
    (
      SELECT count(*)
      FROM simulations s
      WHERE s.created_at >= now() - w.span
        AND NOT s.success
    ) AS simulation_fail,
    (
      SELECT count(*)
      FROM simulations s
      WHERE s.created_at >= now() - w.span
        AND NOT s.success
        AND COALESCE(s.revert_reason, '') ILIKE '%MinProfitNotMet%'
    ) AS min_profit_not_met,
    (
      SELECT count(*)
      FROM transactions t
      WHERE t.created_at >= now() - w.span
        AND t.tx_hash IS NOT NULL
    ) AS submitted_txs,
    (
      SELECT count(*)
      FROM transactions t
      WHERE t.created_at >= now() - w.span
        AND t.status = 'Reverted'
    ) AS reverted_txs
  FROM windows w
)
SELECT
  label,
  opportunities,
  simulations,
  simulation_success,
  simulation_fail,
  min_profit_not_met,
  CASE
    WHEN simulations > 0 THEN round(100.0 * min_profit_not_met / simulations, 2)
    ELSE 0
  END AS minprofit_pct_of_sims,
  submitted_txs,
  reverted_txs
FROM funnel
ORDER BY span;

CREATE TEMP TABLE mp_base AS
SELECT
  s.id AS simulation_id,
  s.opportunity_id,
  s.created_at AS simulation_at,
  s.block_number AS simulation_block,
  s.success AS simulation_success,
  s.revert_reason AS simulation_revert_reason,
  NULLIF(s.simulated_profit, '')::numeric AS simulated_profit,
  NULLIF(s.net_simulated_profit, '')::numeric AS net_simulated_profit,
  o.created_at AS opportunity_at,
  o.block_number AS opportunity_block,
  o.strategy,
  COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
  lower(COALESCE(s.token_in, o.token_in)) AS token_in,
  COALESCE(NULLIF(s.amount_in, ''), NULLIF(o.amount_in, '')) AS amount_in,
  COALESCE(NULLIF(s.expected_profit, ''), NULLIF(o.expected_profit, ''))::numeric AS expected_profit,
  COALESCE(NULLIF(s.min_profit, ''), NULLIF(o.min_profit, ''))::numeric AS min_profit,
  o.path_json,
  (
    SELECT count(*)
    FROM transactions t
    WHERE t.opportunity_id = o.id
  ) AS tx_records,
  (
    SELECT count(*)
    FROM transactions t
    WHERE t.opportunity_id = o.id
      AND t.tx_hash IS NOT NULL
  ) AS submitted_records,
  (
    SELECT count(*)
    FROM transactions t
    WHERE t.opportunity_id = o.id
      AND t.status = 'Reverted'
  ) AS reverted_records,
  (
    SELECT max(t.created_at)
    FROM transactions t
    WHERE t.opportunity_id = o.id
  ) AS latest_tx_at
FROM simulations s
JOIN opportunities o ON o.id = s.opportunity_id
WHERE s.created_at >= now() - :'interval'::interval
  AND s.success = false
  AND COALESCE(s.revert_reason, '') ILIKE '%MinProfitNotMet%';

CREATE INDEX mp_base_sim_idx ON mp_base(simulation_id);
CREATE INDEX mp_base_opp_idx ON mp_base(opportunity_id);
CREATE INDEX mp_base_path_idx ON mp_base(path_name);

CREATE TEMP TABLE mp_steps AS
SELECT
  b.simulation_id,
  b.opportunity_id,
  x.ord::int AS step_no,
  COALESCE(x.step->>'dex', x.step->>'dex_kind', '-') AS dex,
  COALESCE(x.step->>'variant', x.step->>'pool_variant', '-') AS variant,
  lower(COALESCE(x.step->>'pool', x.step->>'pool_address')) AS pool,
  lower(COALESCE(x.step->>'factory', x.step->>'factory_address')) AS factory,
  lower(COALESCE(x.step->>'token_in', x.step->>'tokenIn')) AS token_in,
  lower(COALESCE(x.step->>'token_out', x.step->>'tokenOut')) AS token_out,
  x.step AS raw_step
FROM mp_base b
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.path_json->'steps', '[]'::jsonb)) WITH ORDINALITY AS x(step, ord);

CREATE INDEX mp_steps_sim_idx ON mp_steps(simulation_id);
CREATE INDEX mp_steps_pool_idx ON mp_steps(pool);

CREATE TEMP TABLE mp_features AS
SELECT
  b.*,
  COALESCE(f.path_len, 0) AS path_len,
  COALESCE(f.protocol_combo, '-') AS protocol_combo,
  COALESCE(f.path_signature, '-') AS path_signature,
  COALESCE(f.has_v4, false) AS has_v4,
  COALESCE(f.has_balancer, false) AS has_balancer,
  COALESCE(f.has_v3_style, false) AS has_v3_style,
  COALESCE(f.has_aero_classic, false) AS has_aero_classic,
  extract(epoch FROM b.simulation_at - b.opportunity_at) AS sim_age_secs,
  CASE
    WHEN b.simulation_block IS NOT NULL THEN b.simulation_block - b.opportunity_block
    ELSE NULL
  END AS sim_block_lag,
  CASE
    WHEN b.min_profit > 0 THEN b.expected_profit / b.min_profit
    ELSE NULL
  END AS expected_to_min_ratio
FROM mp_base b
LEFT JOIN (
  SELECT
    simulation_id,
    count(*) AS path_len,
    string_agg(dex || ':' || variant, ' -> ' ORDER BY step_no) AS protocol_combo,
    string_agg(dex || ':' || variant || ':' || right(COALESCE(pool, ''), 6), ' -> ' ORDER BY step_no) AS path_signature,
    bool_or(variant ILIKE '%UniswapV4%' OR dex ILIKE '%UniswapV4%') AS has_v4,
    bool_or(variant ILIKE '%Balancer%' OR dex ILIKE '%Balancer%') AS has_balancer,
    bool_or(
      variant ILIKE '%UniswapV3%'
      OR variant ILIKE '%PancakeV3%'
      OR variant ILIKE '%Slipstream%'
      OR variant ILIKE '%UniswapV4%'
    ) AS has_v3_style,
    bool_or(variant ILIKE '%AerodromeVolatile%') AS has_aero_classic
  FROM mp_steps
  GROUP BY simulation_id
) f ON f.simulation_id = b.simulation_id;

CREATE INDEX mp_features_path_idx ON mp_features(path_name);
CREATE INDEX mp_features_protocol_idx ON mp_features(protocol_combo);

\echo
\echo '================================================================================'
\echo '2. MinProfitNotMet summary in selected interval'
\echo '================================================================================'
SELECT
  count(*) AS minprofit_failures,
  min(simulation_at) AS first_failure,
  max(simulation_at) AS latest_failure,
  count(DISTINCT opportunity_id) AS opportunities,
  count(*) FILTER (WHERE submitted_records > 0) AS failures_with_tx_record,
  count(*) FILTER (WHERE submitted_records > 0 AND reverted_records > 0) AS failures_with_reverted_tx,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY sim_age_secs) AS p50_sim_age_secs,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY sim_age_secs) AS p90_sim_age_secs,
  max(sim_age_secs) AS max_sim_age_secs,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY sim_block_lag) AS p50_sim_block_lag,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY sim_block_lag) AS p90_sim_block_lag,
  max(sim_block_lag) AS max_sim_block_lag,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p50_expected_to_min_ratio,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p90_expected_to_min_ratio,
  max(expected_to_min_ratio) AS max_expected_to_min_ratio
FROM mp_features;

\echo
\echo '================================================================================'
\echo '3. MinProfitNotMet by path length and protocol combo'
\echo '================================================================================'
SELECT
  path_len,
  protocol_combo,
  count(*) AS failures,
  count(DISTINCT path_name) AS paths,
  max(simulation_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p50_expected_to_min_ratio,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p90_expected_to_min_ratio,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY sim_age_secs) AS p90_sim_age_secs
FROM mp_features
GROUP BY 1, 2
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 40;

\echo
\echo '================================================================================'
\echo '4. top failing paths'
\echo '================================================================================'
SELECT
  path_name,
  path_len,
  protocol_combo,
  count(*) AS failures,
  max(simulation_at) AS latest,
  min(expected_profit) AS min_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit,
  min(expected_to_min_ratio) AS min_expected_to_min_ratio,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p50_expected_to_min_ratio,
  max(expected_to_min_ratio) AS max_expected_to_min_ratio,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY sim_age_secs) AS p90_sim_age_secs,
  max(sim_block_lag) AS max_sim_block_lag
FROM mp_features
GROUP BY 1, 2, 3
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 50;

\echo
\echo '================================================================================'
\echo '5. expected/min-profit margin buckets'
\echo '================================================================================'
SELECT
  CASE
    WHEN expected_to_min_ratio IS NULL THEN 'unknown'
    WHEN expected_to_min_ratio < 1.05 THEN '<1.05x'
    WHEN expected_to_min_ratio < 1.2 THEN '1.05-1.2x'
    WHEN expected_to_min_ratio < 2 THEN '1.2-2x'
    WHEN expected_to_min_ratio < 5 THEN '2-5x'
    WHEN expected_to_min_ratio < 20 THEN '5-20x'
    ELSE '>=20x'
  END AS margin_bucket,
  count(*) AS failures,
  max(simulation_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY sim_age_secs) AS p50_sim_age_secs,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY sim_age_secs) AS p90_sim_age_secs
FROM mp_features
GROUP BY 1
ORDER BY
  CASE
    WHEN margin_bucket = '<1.05x' THEN 1
    WHEN margin_bucket = '1.05-1.2x' THEN 2
    WHEN margin_bucket = '1.2-2x' THEN 3
    WHEN margin_bucket = '2-5x' THEN 4
    WHEN margin_bucket = '5-20x' THEN 5
    WHEN margin_bucket = '>=20x' THEN 6
    ELSE 7
  END;

\echo
\echo '================================================================================'
\echo '6. simulation age and block-lag buckets'
\echo '================================================================================'
SELECT
  CASE
    WHEN sim_age_secs IS NULL THEN 'unknown'
    WHEN sim_age_secs < 0.25 THEN '<250ms'
    WHEN sim_age_secs < 0.5 THEN '250-500ms'
    WHEN sim_age_secs < 1 THEN '500ms-1s'
    WHEN sim_age_secs < 2 THEN '1-2s'
    WHEN sim_age_secs < 5 THEN '2-5s'
    ELSE '>=5s'
  END AS sim_age_bucket,
  CASE
    WHEN sim_block_lag IS NULL THEN 'unknown'
    WHEN sim_block_lag <= 0 THEN 'same_or_earlier'
    WHEN sim_block_lag = 1 THEN '1 block'
    WHEN sim_block_lag <= 3 THEN '2-3 blocks'
    ELSE '>=4 blocks'
  END AS sim_block_lag_bucket,
  count(*) AS failures,
  max(simulation_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p50_expected_to_min_ratio,
  max(expected_to_min_ratio) AS max_expected_to_min_ratio
FROM mp_features
GROUP BY 1, 2
ORDER BY failures DESC, latest DESC;

\echo
\echo '================================================================================'
\echo '7. token and amount buckets'
\echo '================================================================================'
SELECT
  token_in,
  amount_in,
  count(*) AS failures,
  count(DISTINCT path_name) AS paths,
  max(simulation_at) AS latest,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit) AS p50_expected_profit,
  max(expected_profit) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_to_min_ratio) AS p50_expected_to_min_ratio
FROM mp_features
GROUP BY 1, 2
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 50;

\echo
\echo '================================================================================'
\echo '8. top pools in MinProfitNotMet paths'
\echo '================================================================================'
SELECT
  s.pool,
  COALESCE(p.dex, po.dex, s.dex) AS dex,
  COALESCE(p.variant, po.variant, s.variant) AS variant,
  COALESCE(tp.symbol, po.symbol, '-') AS symbol,
  count(*) AS step_failures,
  count(DISTINCT s.opportunity_id) AS opportunities,
  count(DISTINCT b.path_name) AS paths,
  max(b.simulation_at) AS latest,
  max(b.expected_profit) AS max_expected_profit,
  max(b.expected_to_min_ratio) AS max_expected_to_min_ratio,
  max(ps.block_number) AS latest_state_block,
  max(ps.updated_at) AS latest_state_at
FROM mp_steps s
JOIN mp_features b ON b.simulation_id = s.simulation_id
LEFT JOIN pools p ON lower(p.pool_address) = s.pool
LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
LEFT JOIN LATERAL (
  SELECT po.dex, po.variant, po.symbol
  FROM protocol_pool_observations po
  WHERE lower(po.pool_address) = s.pool
  ORDER BY po.updated_at DESC NULLS LAST
  LIMIT 1
) po ON true
LEFT JOIN LATERAL (
  SELECT ps.block_number, ps.updated_at
  FROM pool_states ps
  WHERE lower(ps.pool_address) = s.pool
  ORDER BY ps.block_number DESC NULLS LAST, ps.updated_at DESC NULLS LAST
  LIMIT 1
) ps ON true
GROUP BY 1, 2, 3, 4
ORDER BY step_failures DESC, max_expected_profit DESC
LIMIT 80;

\echo
\echo '================================================================================'
\echo '9. path diagnostics flags recorded by searcher'
\echo '================================================================================'
SELECT
  COALESCE(path_json->'diagnostics'->>'modes', '-') AS quote_modes,
  COALESCE(path_json->'diagnostics'->>'tick_range_exhausted', '-') AS tick_range_exhausted,
  COALESCE(path_json->'diagnostics'->>'v3_pools_without_ticks', '-') AS v3_pools_without_ticks,
  COALESCE(path_json->'diagnostics'->>'ticks_used', '-') AS ticks_used,
  COALESCE(path_json->'diagnostics'->>'crossed_ticks', '-') AS crossed_ticks,
  count(*) AS failures,
  max(simulation_at) AS latest,
  max(expected_profit) AS max_expected_profit
FROM mp_features
GROUP BY 1, 2, 3, 4, 5
ORDER BY failures DESC, max_expected_profit DESC
LIMIT 50;

\echo
\echo '================================================================================'
\echo '10. same-path success controls in selected interval'
\echo '================================================================================'
WITH mp_paths AS (
  SELECT DISTINCT path_name FROM mp_features
),
all_sims AS (
  SELECT
    COALESCE(s.path_name, o.path_json->>'name', '-') AS path_name,
    s.created_at,
    s.success,
    s.revert_reason,
    COALESCE(NULLIF(s.expected_profit, ''), NULLIF(o.expected_profit, ''))::numeric AS expected_profit,
    COALESCE(NULLIF(s.min_profit, ''), NULLIF(o.min_profit, ''))::numeric AS min_profit
  FROM simulations s
  JOIN opportunities o ON o.id = s.opportunity_id
  JOIN mp_paths p ON p.path_name = COALESCE(s.path_name, o.path_json->>'name', '-')
  WHERE s.created_at >= now() - :'interval'::interval
)
SELECT
  path_name,
  count(*) FILTER (WHERE success) AS successes,
  count(*) FILTER (WHERE NOT success AND COALESCE(revert_reason, '') ILIKE '%MinProfitNotMet%') AS minprofit_failures,
  count(*) FILTER (WHERE NOT success AND COALESCE(revert_reason, '') NOT ILIKE '%MinProfitNotMet%') AS other_failures,
  max(created_at) AS latest,
  max(expected_profit) AS max_expected_profit,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY expected_profit / NULLIF(min_profit, 0)) AS p50_expected_to_min_ratio
FROM all_sims
GROUP BY 1
ORDER BY minprofit_failures DESC, successes DESC, max_expected_profit DESC
LIMIT 60;

\echo
\echo '================================================================================'
\echo '11. representative replay targets'
\echo '================================================================================'
WITH ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY protocol_combo
      ORDER BY expected_profit DESC, simulation_at DESC
    ) AS rn_combo,
    row_number() OVER (
      PARTITION BY path_name
      ORDER BY expected_profit DESC, simulation_at DESC
    ) AS rn_path
  FROM mp_features
)
SELECT
  simulation_at,
  opportunity_at,
  opportunity_block,
  simulation_block,
  simulation_id,
  opportunity_id,
  path_name,
  path_len,
  protocol_combo,
  token_in,
  amount_in,
  expected_profit,
  min_profit,
  expected_to_min_ratio,
  sim_age_secs,
  sim_block_lag,
  'cargo run -p base-arb-recorder --bin replay_simulations -- --opportunity-id ' || opportunity_id || ' --out reports/replay-' || left(opportunity_id::text, 8) || '.txt' AS replay_command
FROM ranked
WHERE rn_combo <= 3
  AND rn_path <= 2
ORDER BY expected_profit DESC, simulation_at DESC
LIMIT 40;

\echo
\echo '================================================================================'
\echo '12. pool state freshness at opportunity block'
\echo '================================================================================'
WITH state_at_opp AS (
  SELECT DISTINCT ON (s.simulation_id, s.step_no)
    s.simulation_id,
    s.step_no,
    s.pool,
    b.protocol_combo,
    b.path_name,
    b.opportunity_at,
    b.opportunity_block,
    b.expected_profit,
    ps.source,
    ps.block_number AS state_block,
    ps.updated_at AS state_updated_at
  FROM mp_steps s
  JOIN mp_features b ON b.simulation_id = s.simulation_id
  LEFT JOIN pool_states ps
    ON lower(ps.pool_address) = s.pool
   AND ps.block_number <= b.opportunity_block
  ORDER BY s.simulation_id, s.step_no, ps.block_number DESC NULLS LAST, ps.updated_at DESC NULLS LAST
)
SELECT
  COALESCE(source, 'no_pool_state_match') AS state_source,
  count(*) AS step_count,
  count(DISTINCT simulation_id) AS simulations,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY opportunity_block - state_block) AS p50_state_lag_blocks_at_opp,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY opportunity_block - state_block) AS p90_state_lag_blocks_at_opp,
  max(opportunity_block - state_block) AS max_state_lag_blocks_at_opp,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM opportunity_at - state_updated_at)) AS p50_state_age_secs_at_opp,
  percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM opportunity_at - state_updated_at)) AS p90_state_age_secs_at_opp,
  max(expected_profit) AS max_expected_profit
FROM state_at_opp
GROUP BY 1
ORDER BY step_count DESC, max_expected_profit DESC;

\echo
\echo '================================================================================'
\echo '13. newest raw samples'
\echo '================================================================================'
SELECT
  simulation_at,
  opportunity_at,
  opportunity_block,
  simulation_block,
  simulation_id,
  opportunity_id,
  path_name,
  path_signature,
  token_in,
  amount_in,
  expected_profit,
  min_profit,
  expected_to_min_ratio,
  sim_age_secs,
  sim_block_lag,
  submitted_records,
  reverted_records
FROM mp_features
ORDER BY simulation_at DESC
LIMIT 80;
SQL

cat <<EOF
wrote $OUT_FILE

next:
  1. open the report and identify the dominant bucket/path/protocol.
  2. replay representative opportunities from section 11:
     cargo run -p base-arb-recorder --bin replay_simulations -- --opportunity-id <uuid> --out reports/replay-<short>.txt
EOF
