use std::{env, fmt::Write as _, fs, path::PathBuf};

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{
        ArbPath, DexKind, PoolRegistryEntry, PoolState, PoolVariant, QuoteStepDiagnostics, SwapStep,
    },
};
use base_arb_dex::{
    aerodrome::{AerodromeStableQuoter, AerodromeVolatileQuoter},
    quoter::DexQuoter,
};
use base_arb_storage::{redis::RedisStore, PoolStateStore, TickStateStore};
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Verdict {
    Clean,
    MissingRecordedDiagnostics,
    HistoricalStateFetchFailed,
    ClassicStateDrift,
    ClassicStateStaleByOpportunityBlock,
    ClassicFormulaMismatch,
    ClassicPoolFormulaMismatch,
    ClassicKNotStateOrFormula,
    V3StateOrTickNeedsReview,
    UnsupportedDeepCheck,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Self::Clean => "clean_or_not_enough_evidence",
            Self::MissingRecordedDiagnostics => "missing_recorded_quote_diagnostics",
            Self::HistoricalStateFetchFailed => "historical_state_fetch_failed",
            Self::ClassicStateDrift => "classic_state_drift",
            Self::ClassicStateStaleByOpportunityBlock => {
                "classic_state_stale_by_opportunity_block"
            }
            Self::ClassicFormulaMismatch => "classic_formula_mismatch",
            Self::ClassicPoolFormulaMismatch => "classic_pool_formula_mismatch",
            Self::ClassicKNotStateOrFormula => "classic_k_not_state_or_formula",
            Self::V3StateOrTickNeedsReview => "v3_state_or_tick_needs_review",
            Self::UnsupportedDeepCheck => "unsupported_deep_check",
        }
    }
}

#[derive(Debug)]
struct Cli {
    opportunity_id: Option<Uuid>,
    simulation_id: Option<Uuid>,
    out: Option<PathBuf>,
}

#[derive(Debug, FromRow)]
struct CaseRow {
    opportunity_id: Uuid,
    opportunity_at: DateTime<Utc>,
    opportunity_block: i64,
    strategy: String,
    token_in: String,
    amount_in: String,
    expected_profit: String,
    min_profit: String,
    path_json: serde_json::Value,
    simulation_id: Option<Uuid>,
    simulation_at: Option<DateTime<Utc>>,
    simulation_block: Option<i64>,
    simulation_success: Option<bool>,
    simulation_revert_reason: Option<String>,
}

#[derive(Debug, FromRow)]
struct TickCountRow {
    ticks: i64,
    latest_tick_block: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let pg = PgPool::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);
    let case = load_case(&pg, &cli).await?;
    let path: ArbPath = serde_json::from_value(case.path_json.clone())
        .with_context(|| format!("failed to decode path_json for {}", case.opportunity_id))?;

    let mut report = String::new();
    let verdict = write_report(&mut report, &pg, &redis, &provider, &case, &path).await?;
    writeln!(report)?;
    writeln!(report, "== Verdict ==")?;
    writeln!(report, "verdict={}", verdict.label())?;
    writeln!(report, "next_action={}", next_action(verdict))?;

    if let Some(out) = cli.out {
        if let Some(parent) = out.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
        }
        fs::write(&out, report).with_context(|| format!("failed to write {}", out.display()))?;
        println!("wrote {}", out.display());
    } else {
        print!("{report}");
    }
    Ok(())
}

async fn write_report(
    report: &mut String,
    pg: &PgPool,
    redis: &RedisStore,
    provider: &ChainProvider,
    case: &CaseRow,
    path: &ArbPath,
) -> Result<Verdict> {
    writeln!(report, "arb doctor state/quote report")?;
    writeln!(report, "generated_at_utc={}", Utc::now())?;
    writeln!(report, "opportunity_id={}", case.opportunity_id)?;
    writeln!(report, "opportunity_at={}", case.opportunity_at)?;
    writeln!(report, "opportunity_block={}", case.opportunity_block)?;
    writeln!(report, "strategy={}", case.strategy)?;
    writeln!(
        report,
        "simulation_id={}",
        case.simulation_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        report,
        "simulation_at={}",
        case.simulation_at
            .map(|ts| ts.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        report,
        "simulation_block={}",
        case.simulation_block
            .map(|block| block.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        report,
        "simulation_success={}",
        case.simulation_success
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        report,
        "simulation_revert_reason={}",
        case.simulation_revert_reason.as_deref().unwrap_or("-")
    )?;
    writeln!(report, "token_in={}", case.token_in)?;
    writeln!(report, "amount_in={}", case.amount_in)?;
    writeln!(report, "expected_profit={}", case.expected_profit)?;
    writeln!(report, "min_profit={}", case.min_profit)?;
    writeln!(report, "path_name={}", path.name)?;
    writeln!(report, "path_len={}", path.steps.len())?;

    let mut verdict = if path
        .diagnostics
        .as_ref()
        .is_some_and(|diagnostics| !diagnostics.steps.is_empty())
    {
        Verdict::Clean
    } else {
        Verdict::MissingRecordedDiagnostics
    };

    for (idx, step) in path.steps.iter().enumerate() {
        let step_verdict =
            analyze_step(report, pg, redis, provider, case, path, step, idx + 1).await?;
        verdict = verdict.max(step_verdict);
    }

    Ok(refine_path_verdict(verdict, case))
}

#[allow(clippy::too_many_arguments)]
async fn analyze_step(
    report: &mut String,
    pg: &PgPool,
    redis: &RedisStore,
    provider: &ChainProvider,
    case: &CaseRow,
    path: &ArbPath,
    step: &SwapStep,
    step_no: usize,
) -> Result<Verdict> {
    writeln!(report)?;
    writeln!(report, "== Step {step_no} ==")?;
    writeln!(report, "dex={:?}", step.dex)?;
    writeln!(report, "variant={:?}", step.variant)?;
    writeln!(report, "pool={:#x}", step.pool)?;
    writeln!(report, "factory={:?}", step.factory_address)?;
    writeln!(report, "token_in={:#x}", step.token_in)?;
    writeln!(report, "token_out={:#x}", step.token_out)?;
    writeln!(report, "fee_bps={:?}", step.fee_bps)?;
    writeln!(report, "pool_key_fee_pips={:?}", step.pool_key_fee_pips)?;
    writeln!(report, "stable={:?}", step.stable)?;
    writeln!(report, "tick_spacing={:?}", step.tick_spacing)?;

    let pg_ticks = load_pg_tick_count(pg, step.pool).await?;
    writeln!(
        report,
        "pg_ticks={} pg_latest_tick_block={}",
        pg_ticks.ticks,
        pg_ticks
            .latest_tick_block
            .map(|block| block.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    let redis_ticks = redis.get_pool_ticks(step.pool).await?;
    writeln!(report, "redis_ticks={}", redis_ticks.len())?;
    if let Some(current) = redis.get_pool_state(step.pool).await? {
        write_state_line(report, "redis_current_state", &current)?;
    } else {
        writeln!(report, "redis_current_state=missing")?;
    }

    let Some(recorded) = recorded_step_diagnostics(path, step_no, step) else {
        writeln!(report, "recorded_snapshot=missing")?;
        return Ok(match step.variant.unwrap_or(default_variant(step.dex)) {
            PoolVariant::AerodromeVolatile
            | PoolVariant::AerodromeSlipstream
            | PoolVariant::UniswapV3
            | PoolVariant::PancakeV3
            | PoolVariant::UniswapV4 => Verdict::MissingRecordedDiagnostics,
            PoolVariant::BalancerV3 => Verdict::UnsupportedDeepCheck,
        });
    };

    write_recorded_snapshot(report, recorded)?;
    let source_block = recorded.source_block;
    let source_hash = match provider.get_block_hash(source_block).await {
        Ok(hash) => hash,
        Err(err) => {
            writeln!(
                report,
                "historical_source_state=failed block={} error={err:#}",
                source_block
            )?;
            return Ok(Verdict::HistoricalStateFetchFailed);
        }
    };

    let entry = registry_entry(step, recorded);
    let source_state = match provider
        .fetch_pool_state_from_registry_at_block_hash(&entry, &source_hash, source_block)
        .await
    {
        Ok(state) => {
            write_state_line(report, "onchain_source_state", &state)?;
            Some(state)
        }
        Err(err) => {
            writeln!(
                report,
                "onchain_source_state=failed block={} hash={} error={err:#}",
                source_block, source_hash
            )?;
            None
        }
    };

    let mut opportunity_state = None;
    if case.opportunity_block >= 0 && case.opportunity_block as u64 != source_block {
        let opportunity_block = case.opportunity_block as u64;
        match provider.get_block_hash(opportunity_block).await {
            Ok(hash) => match provider
                .fetch_pool_state_from_registry_at_block_hash(&entry, &hash, opportunity_block)
                .await
            {
                Ok(state) => {
                    write_state_line(report, "onchain_opportunity_state", &state)?;
                    opportunity_state = Some(state);
                }
                Err(err) => writeln!(
                    report,
                    "onchain_opportunity_state=failed block={} hash={} error={err:#}",
                    opportunity_block, hash
                )?,
            },
            Err(err) => writeln!(
                report,
                "onchain_opportunity_state=failed block={} error={err:#}",
                opportunity_block
            )?,
        }
    }

    let Some(source_state) = source_state else {
        return Ok(Verdict::HistoricalStateFetchFailed);
    };

    let state_verdict = compare_recorded_to_onchain(
        report,
        "recorded_vs_onchain_source",
        recorded,
        &source_state,
        Verdict::ClassicStateDrift,
    )?;
    let opportunity_verdict = if let Some(opportunity_state) = opportunity_state.as_ref() {
        compare_recorded_to_onchain(
            report,
            "recorded_vs_onchain_opportunity",
            recorded,
            opportunity_state,
            Verdict::ClassicStateStaleByOpportunityBlock,
        )?
    } else {
        Verdict::Clean
    };
    let quote_verdict = quote_check(
        report,
        step,
        recorded,
        &source_state,
        provider,
        &source_hash,
    )
    .await
    .unwrap_or_else(|err| {
        let _ = writeln!(report, "quote_check=failed error={err:#}");
        Verdict::HistoricalStateFetchFailed
    });

    Ok(state_verdict.max(opportunity_verdict).max(quote_verdict))
}

fn write_recorded_snapshot(report: &mut String, recorded: &QuoteStepDiagnostics) -> Result<()> {
    writeln!(
        report,
        "recorded_snapshot=mode:{} source_block:{} valid_through_block:{} amount_in:{} amount_out:{} fee_bps:{} fee_pips:{:?} reserve0:{:?} reserve1:{:?} sqrt:{:?} liquidity:{:?} tick:{:?} tick_count:{} ticks_used:{} crossed_ticks:{} exhausted:{}",
        recorded.mode,
        recorded.source_block,
        recorded.valid_through_block.max(recorded.source_block),
        recorded.amount_in,
        recorded.amount_out,
        recorded.fee_bps,
        recorded.fee_pips,
        recorded.reserve0,
        recorded.reserve1,
        recorded.sqrt_price_x96,
        recorded.liquidity,
        recorded.tick,
        recorded.tick_count,
        recorded.ticks_used,
        recorded.crossed_ticks,
        recorded.tick_range_exhausted
    )?;
    Ok(())
}

fn write_state_line(report: &mut String, label: &str, state: &PoolState) -> Result<()> {
    writeln!(
        report,
        "{label}=block:{} valid_through:{} variant:{:?} token0:{:#x} token1:{:#x} fee_bps:{} fee_pips:{:?} stable:{:?} reserve0:{:?} reserve1:{:?} sqrt:{:?} liquidity:{:?} tick:{:?}",
        state.block_number,
        state.effective_valid_through_block(),
        state.variant,
        state.token0,
        state.token1,
        state.fee_bps,
        state.fee_pips,
        state.stable,
        state.reserve0,
        state.reserve1,
        state.sqrt_price_x96,
        state.liquidity,
        state.tick
    )?;
    Ok(())
}

fn compare_recorded_to_onchain(
    report: &mut String,
    label: &str,
    recorded: &QuoteStepDiagnostics,
    onchain: &PoolState,
    classic_drift_verdict: Verdict,
) -> Result<Verdict> {
    let mut verdict = Verdict::Clean;
    let reserve0_diff = diff_bps_opt(recorded.reserve0, onchain.reserve0);
    let reserve1_diff = diff_bps_opt(recorded.reserve1, onchain.reserve1);
    let sqrt_diff = diff_bps_opt(recorded.sqrt_price_x96, onchain.sqrt_price_x96);
    let liquidity_diff = diff_bps_opt(recorded.liquidity, onchain.liquidity);
    let tick_diff = match (recorded.tick, onchain.tick) {
        (Some(left), Some(right)) => Some((i64::from(left) - i64::from(right)).abs()),
        _ => None,
    };

    writeln!(
        report,
        "{label}=reserve0_diff_bps:{} reserve1_diff_bps:{} sqrt_diff_bps:{} liquidity_diff_bps:{} tick_diff:{} fee_bps_recorded:{} fee_bps_onchain:{} fee_pips_recorded:{:?} fee_pips_onchain:{:?}",
        fmt_opt_u256(reserve0_diff),
        fmt_opt_u256(reserve1_diff),
        fmt_opt_u256(sqrt_diff),
        fmt_opt_u256(liquidity_diff),
        tick_diff
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        recorded.fee_bps,
        onchain.fee_bps,
        recorded.fee_pips,
        onchain.fee_pips
    )?;

    if matches!(recorded.variant, PoolVariant::AerodromeVolatile) {
        if reserve0_diff.is_some_and(|value| !value.is_zero())
            || reserve1_diff.is_some_and(|value| !value.is_zero())
            || recorded.fee_bps != onchain.fee_bps
        {
            verdict = classic_drift_verdict;
        }
    } else if sqrt_diff.is_some_and(|value| !value.is_zero())
        || liquidity_diff.is_some_and(|value| !value.is_zero())
        || tick_diff.is_some_and(|value| value != 0)
    {
        verdict = Verdict::V3StateOrTickNeedsReview;
    }
    Ok(verdict)
}

async fn quote_check(
    report: &mut String,
    step: &SwapStep,
    recorded: &QuoteStepDiagnostics,
    source_state: &PoolState,
    provider: &ChainProvider,
    source_hash: &str,
) -> Result<Verdict> {
    match recorded.variant {
        PoolVariant::AerodromeVolatile => {
            let mut recorded_state = source_state.clone();
            recorded_state.reserve0 = recorded.reserve0;
            recorded_state.reserve1 = recorded.reserve1;
            recorded_state.fee_bps = recorded.fee_bps;
            recorded_state.fee_pips = recorded.fee_pips;
            recorded_state.stable = recorded.stable;

            let recorded_snapshot_quote =
                quote_classic(&recorded_state, step.token_in, recorded.amount_in).await?;
            let onchain_state_quote =
                quote_classic(source_state, step.token_in, recorded.amount_in).await?;
            let pool_get_amount_out = pool_get_amount_out_at_block_hash(
                provider,
                step.pool,
                recorded.amount_in,
                step.token_in,
                source_hash,
            )
            .await;

            let recorded_quote_diff = diff_bps(recorded.amount_out, recorded_snapshot_quote);
            let onchain_quote_diff = diff_bps(recorded.amount_out, onchain_state_quote);
            writeln!(
                report,
                "classic_quote_check=amount_in:{} recorded_amount_out:{} quote_from_recorded_snapshot:{} recorded_formula_diff_bps:{} quote_from_onchain_source:{} onchain_source_diff_bps:{}",
                recorded.amount_in,
                recorded.amount_out,
                recorded_snapshot_quote,
                recorded_quote_diff,
                onchain_state_quote,
                onchain_quote_diff
            )?;

            let mut verdict = Verdict::Clean;
            if !recorded_quote_diff.is_zero() {
                verdict = Verdict::ClassicFormulaMismatch;
            } else if !onchain_quote_diff.is_zero() {
                verdict = Verdict::ClassicStateDrift;
            }

            match pool_get_amount_out {
                Ok(pool_quote) => {
                    let pool_diff = diff_bps(onchain_state_quote, pool_quote);
                    writeln!(
                        report,
                        "classic_pool_getAmountOut=amount_out:{} local_vs_pool_diff_bps:{}",
                        pool_quote, pool_diff
                    )?;
                    if !pool_diff.is_zero() && verdict < Verdict::ClassicPoolFormulaMismatch {
                        verdict = Verdict::ClassicPoolFormulaMismatch;
                    }
                }
                Err(err) => {
                    writeln!(report, "classic_pool_getAmountOut=failed error={err:#}")?;
                }
            }
            Ok(verdict)
        }
        PoolVariant::AerodromeSlipstream
        | PoolVariant::UniswapV3
        | PoolVariant::PancakeV3
        | PoolVariant::UniswapV4 => {
            writeln!(
                report,
                "v3_style_quote_check=recorded_amount_in:{} recorded_amount_out:{} note:historical_tick_set_not_replayed_here",
                recorded.amount_in, recorded.amount_out
            )?;
            if recorded.tick_count == 0 || recorded.tick_range_exhausted {
                Ok(Verdict::V3StateOrTickNeedsReview)
            } else {
                Ok(Verdict::Clean)
            }
        }
        PoolVariant::BalancerV3 => {
            writeln!(
                report,
                "balancer_quote_check=unsupported_deep_check recorded_amount_in:{} recorded_amount_out:{}",
                recorded.amount_in, recorded.amount_out
            )?;
            Ok(Verdict::UnsupportedDeepCheck)
        }
    }
}

async fn quote_classic(state: &PoolState, token_in: Address, amount_in: U256) -> Result<U256> {
    let quote = if state.stable.unwrap_or(false) {
        AerodromeStableQuoter
            .quote_exact_in(state, token_in, amount_in)
            .await?
    } else {
        AerodromeVolatileQuoter
            .quote_exact_in(state, token_in, amount_in)
            .await?
    };
    Ok(quote.amount_out)
}

async fn pool_get_amount_out_at_block_hash(
    provider: &ChainProvider,
    pool: Address,
    amount_in: U256,
    token_in: Address,
    block_hash: &str,
) -> Result<U256> {
    let data = encode_pool_get_amount_out(amount_in, token_in);
    let raw = provider
        .eth_call_from_at_block_hash(
            None,
            pool,
            &format!("0x{}", hex::encode(data)),
            "Aerodrome pool getAmountOut",
            block_hash,
        )
        .await?;
    decode_single_uint(&raw).context("failed to decode pool getAmountOut")
}

fn refine_path_verdict(verdict: Verdict, case: &CaseRow) -> Verdict {
    if case
        .simulation_revert_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("UniswapV2: K"))
        && matches!(
            verdict,
            Verdict::Clean | Verdict::V3StateOrTickNeedsReview
        )
    {
        return Verdict::ClassicKNotStateOrFormula;
    }
    verdict
}

fn next_action(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::ClassicStateDrift => {
            "fix market-data classic reserve/fee state maintenance or drift refresh for the affected pool"
        }
        Verdict::ClassicStateStaleByOpportunityBlock => {
            "fix searcher/market-data freshness: this opportunity used an older classic pool snapshot even though onchain reserves had changed by the opportunity block"
        }
        Verdict::ClassicFormulaMismatch => {
            "fix local classic quote math/fee/direction; recorded searcher quote differs from current formula on the recorded snapshot"
        }
        Verdict::ClassicPoolFormulaMismatch => {
            "fix protocol formula/fee source; local classic formula differs from pool getAmountOut at the same block"
        }
        Verdict::ClassicKNotStateOrFormula => {
            "investigate token transfer behavior or intra-block ordering; checked classic state/formula did not explain UniswapV2: K"
        }
        Verdict::V3StateOrTickNeedsReview => {
            "inspect V3-style tick coverage and historical tick replay for the flagged step"
        }
        Verdict::UnsupportedDeepCheck => {
            "add a protocol-specific deep checker under this doctor tool before changing hot-path logic"
        }
        Verdict::MissingRecordedDiagnostics => {
            "ensure searcher emits QuoteDiagnostics for this strategy, then rerun doctor"
        }
        Verdict::HistoricalStateFetchFailed => {
            "verify archive RPC availability for the opportunity/source block and rerun doctor"
        }
        Verdict::Clean => {
            "review executor replay and same-block ordering; doctor did not find a state/formula mismatch"
        }
    }
}

fn registry_entry(step: &SwapStep, recorded: &QuoteStepDiagnostics) -> PoolRegistryEntry {
    PoolRegistryEntry {
        pool_address: step.pool,
        dex: step.dex,
        variant: step.variant.unwrap_or(recorded.variant),
        factory_address: step.factory_address,
        token0: step.token_in,
        token1: step.token_out,
        fee_bps: step.fee_bps.unwrap_or(recorded.fee_bps),
        tick_spacing: step.tick_spacing,
        stable: recorded.stable.or(step.stable),
        enabled: true,
    }
}

async fn load_pg_tick_count(pg: &PgPool, pool: Address) -> Result<TickCountRow> {
    sqlx::query_as::<_, TickCountRow>(
        r#"
        SELECT count(*)::bigint AS ticks, max(block_number)::bigint AS latest_tick_block
        FROM pool_ticks_current
        WHERE lower(pool_address) = lower($1)
        "#,
    )
    .bind(format!("{pool:#x}"))
    .fetch_one(pg)
    .await
    .with_context(|| format!("failed to load tick count for {pool:#x}"))
}

async fn load_case(pg: &PgPool, cli: &Cli) -> Result<CaseRow> {
    if let Some(simulation_id) = cli.simulation_id {
        return sqlx::query_as::<_, CaseRow>(
            r#"
            SELECT
              o.id AS opportunity_id,
              o.created_at AS opportunity_at,
              o.block_number AS opportunity_block,
              o.strategy,
              o.token_in,
              o.amount_in,
              o.expected_profit,
              o.min_profit,
              o.path_json,
              s.id AS simulation_id,
              s.created_at AS simulation_at,
              s.block_number AS simulation_block,
              s.success AS simulation_success,
              s.revert_reason AS simulation_revert_reason
            FROM simulations s
            JOIN opportunities o ON o.id = s.opportunity_id
            WHERE s.id = $1
            "#,
        )
        .bind(simulation_id)
        .fetch_one(pg)
        .await
        .with_context(|| format!("simulation not found: {simulation_id}"));
    }

    if let Some(opportunity_id) = cli.opportunity_id {
        return sqlx::query_as::<_, CaseRow>(
            r#"
            SELECT
              o.id AS opportunity_id,
              o.created_at AS opportunity_at,
              o.block_number AS opportunity_block,
              o.strategy,
              o.token_in,
              o.amount_in,
              o.expected_profit,
              o.min_profit,
              o.path_json,
              s.id AS simulation_id,
              s.created_at AS simulation_at,
              s.block_number AS simulation_block,
              s.success AS simulation_success,
              s.revert_reason AS simulation_revert_reason
            FROM opportunities o
            LEFT JOIN LATERAL (
              SELECT *
              FROM simulations s
              WHERE s.opportunity_id = o.id
              ORDER BY s.created_at DESC
              LIMIT 1
            ) s ON true
            WHERE o.id = $1
            "#,
        )
        .bind(opportunity_id)
        .fetch_one(pg)
        .await
        .with_context(|| format!("opportunity not found: {opportunity_id}"));
    }

    sqlx::query_as::<_, CaseRow>(
        r#"
        SELECT
          o.id AS opportunity_id,
          o.created_at AS opportunity_at,
          o.block_number AS opportunity_block,
          o.strategy,
          o.token_in,
          o.amount_in,
          o.expected_profit,
          o.min_profit,
          o.path_json,
          s.id AS simulation_id,
          s.created_at AS simulation_at,
          s.block_number AS simulation_block,
          s.success AS simulation_success,
          s.revert_reason AS simulation_revert_reason
        FROM simulations s
        JOIN opportunities o ON o.id = s.opportunity_id
        WHERE s.success = false
        ORDER BY s.created_at DESC
        LIMIT 1
        "#,
    )
    .fetch_one(pg)
    .await
    .context("no failed simulation found; pass --opportunity-id or --simulation-id")
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut opportunity_id = None;
    let mut simulation_id = None;
    let mut out = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--opportunity-id" | "--opportunity" => {
                let raw = args.next().context("--opportunity-id requires a UUID")?;
                opportunity_id = Some(Uuid::parse_str(&raw).context("invalid opportunity UUID")?);
            }
            "--simulation-id" | "--simulation" => {
                let raw = args.next().context("--simulation-id requires a UUID")?;
                simulation_id = Some(Uuid::parse_str(&raw).context("invalid simulation UUID")?);
            }
            "--out" => {
                out = Some(PathBuf::from(args.next().context("--out requires a path")?));
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if opportunity_id.is_some() && simulation_id.is_some() {
        bail!("pass only one of --opportunity-id or --simulation-id");
    }
    Ok(Cli {
        opportunity_id,
        simulation_id,
        out,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin arb_doctor -- [--opportunity-id UUID | --simulation-id UUID] [--out reports/doctor/state-diff.txt]"
    );
}

fn recorded_step_diagnostics<'a>(
    path: &'a ArbPath,
    step_no: usize,
    step: &SwapStep,
) -> Option<&'a QuoteStepDiagnostics> {
    path.diagnostics.as_ref()?.steps.iter().find(|recorded| {
        recorded.step_no as usize == step_no
            && recorded.pool == step.pool
            && recorded.token_in == step.token_in
            && recorded.token_out == step.token_out
    })
}

fn default_variant(dex: DexKind) -> PoolVariant {
    match dex {
        DexKind::Aerodrome => PoolVariant::AerodromeVolatile,
        DexKind::UniswapV3 => PoolVariant::UniswapV3,
        DexKind::PancakeSwap => PoolVariant::PancakeV3,
        DexKind::UniswapV4 => PoolVariant::UniswapV4,
        DexKind::Balancer => PoolVariant::BalancerV3,
    }
}

fn diff_bps_opt(left: Option<U256>, right: Option<U256>) -> Option<U256> {
    Some(diff_bps(left?, right?))
}

fn diff_bps(left: U256, right: U256) -> U256 {
    let denominator = left.max(right);
    if denominator.is_zero() {
        return U256::ZERO;
    }
    let diff = if left > right {
        left - right
    } else {
        right - left
    };
    diff.saturating_mul(U256::from(10_000u64)) / denominator
}

fn fmt_opt_u256(value: Option<U256>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}

fn encode_pool_get_amount_out(amount_in: U256, token_in: Address) -> Vec<u8> {
    let mut out = keccak256(b"getAmountOut(uint256,address)")[..4].to_vec();
    out.extend(encode_u256(amount_in));
    out.extend(encode_address(token_in));
    out
}

fn encode_address(address: Address) -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(address.as_slice());
    out
}

fn encode_u256(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn decode_single_uint(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    U256::from_str_radix(&clean[..64], 16).ok()
}
