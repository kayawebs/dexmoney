mod opportunity;
mod risk;
mod strategy;

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::constants::{
    AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_FACTORY, PANCAKE_V3_ROUTER,
};
use base_arb_common::types::{Candidate, DexKind, PoolVariant, SwapStep};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, PairSearchConfigStore,
    PoolStateStore, RecorderStore, TickStateStore,
};
use chrono::Utc;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

const EXECUTOR_DEADLINE_SECS: i64 = 30;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);

    info!("searcher initialized");
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut aggregate = SearchCycleStats::default();
    let mut last_summary = Instant::now();

    loop {
        ticker.tick().await;
        let stats = run_search_cycle(
            &redis,
            &redis,
            &postgres,
            &provider,
            &settings,
            settings.candidate_ttl_ms,
            settings.max_pool_state_age_ms,
            settings.max_price_impact_bps,
            settings.min_expected_profit_usdc,
        )
        .await?;
        aggregate.merge(&stats);
        if last_summary.elapsed() >= Duration::from_secs(30) {
            info!(
                paths = aggregate.search.paths,
                quote_attempts = aggregate.search.quote_attempts,
                quote_successes = aggregate.search.quote_successes,
                quote_skipped = aggregate.search.quote_skipped,
                price_impact_rejected = aggregate.search.price_impact_rejected,
                candidates_emitted = aggregate.search.candidates_emitted,
                risk_rejected = aggregate.risk_rejected,
                opportunities_created = aggregate.opportunities_created,
                best_profit = %aggregate.search.best_profit,
                "searcher cycle summary"
            );
            aggregate = SearchCycleStats::default();
            last_summary = Instant::now();
        }
    }
}

#[derive(Default)]
struct SearchCycleStats {
    search: strategy::SearchStats,
    risk_rejected: u64,
    opportunities_created: u64,
}

impl SearchCycleStats {
    fn merge(&mut self, other: &SearchCycleStats) {
        self.search.merge(&other.search);
        self.risk_rejected += other.risk_rejected;
        self.opportunities_created += other.opportunities_created;
    }
}

async fn run_search_cycle<P, C, R>(
    pool_store: &P,
    candidate_store: &C,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    candidate_ttl_ms: i64,
    max_pool_state_age_ms: i64,
    max_price_impact_bps: u64,
    min_expected_profit_usdc: f64,
) -> Result<SearchCycleStats>
where
    P: PoolStateStore + TickStateStore,
    C: CandidateStore,
    R: RecorderStore + PairSearchConfigStore,
{
    let engine = strategy::engine_from_settings(
        settings,
        candidate_ttl_ms,
        max_price_impact_bps,
        strategy::usdc_to_units(min_expected_profit_usdc),
        recorder.enabled_pair_search_configs().await?,
    )?;
    let pool_states = pool_store.all_pool_states().await?;
    if pool_states.is_empty() {
        debug!("no pool states available in redis");
        return Ok(SearchCycleStats::default());
    }
    let mut tick_states = Vec::new();
    for state in &pool_states {
        if matches!(
            state.variant,
            base_arb_common::types::PoolVariant::AerodromeSlipstream
                | base_arb_common::types::PoolVariant::UniswapV3
                | base_arb_common::types::PoolVariant::PancakeV3
        ) {
            tick_states.extend(pool_store.get_pool_ticks(state.pool_id.address).await?);
        }
    }
    let (candidates, search_stats) = engine.search_with_stats(&pool_states, &tick_states).await?;
    let mut cycle_stats = SearchCycleStats {
        search: search_stats,
        ..SearchCycleStats::default()
    };

    for candidate in candidates {
        debug!(candidate_id = %candidate.id, "quote generated");
        match risk::validate_candidate(
            &candidate,
            &pool_states,
            max_pool_state_age_ms,
            engine.min_expected_profit,
            max_price_impact_bps,
            &engine.whitelist_paths,
        ) {
            Ok(()) => {
                let Some(candidate) =
                    canonicalize_candidate_with_executor(provider, settings, candidate).await?
                else {
                    cycle_stats.risk_rejected += 1;
                    continue;
                };
                recorder.record_opportunity(candidate.clone()).await?;
                candidate_store.push_candidate(candidate.clone()).await?;
                cycle_stats.opportunities_created += 1;
                debug!(candidate_id = %candidate.id, "candidate created");
            }
            Err(err) => {
                cycle_stats.risk_rejected += 1;
                debug!(candidate_id = %candidate.id, reason = %err, "candidate rejected");
            }
        }
    }

    Ok(cycle_stats)
}

async fn canonicalize_candidate_with_executor(
    provider: &ChainProvider,
    settings: &Settings,
    mut candidate: Candidate,
) -> Result<Option<Candidate>> {
    if !settings.searcher_onchain_validate {
        return Ok(Some(candidate));
    }

    let Some(executor) = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
    else {
        debug!("searcher onchain validation skipped: EXECUTOR_CONTRACT is not configured");
        return Ok(Some(candidate));
    };
    let Some(operator) = settings
        .eoa_address_1
        .filter(|address| *address != Address::ZERO)
    else {
        debug!("searcher onchain validation skipped: EOA_ADDRESS_1 is not configured");
        return Ok(Some(candidate));
    };

    let deadline = U256::from((Utc::now().timestamp() + EXECUTOR_DEADLINE_SECS) as u64);
    let calldata = build_execute_calldata(&candidate, candidate.min_profit, deadline, settings)?;
    let data = format!("0x{}", hex::encode(calldata));
    let raw = match provider
        .eth_call_from(
            Some(operator),
            executor,
            &data,
            "Searcher Executor executeWithOwnFunds",
        )
        .await
    {
        Ok(raw) => raw,
        Err(err) => {
            debug!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                error = %err,
                "candidate rejected by executor eth_call"
            );
            return Ok(None);
        }
    };

    let Some(simulated_profit) = decode_uint256_result(&raw) else {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            raw_result = %raw,
            "candidate rejected because executor eth_call returned non-uint256 result"
        );
        return Ok(None);
    };
    if simulated_profit < candidate.min_profit {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            simulated_profit = %simulated_profit,
            min_profit = %candidate.min_profit,
            "candidate rejected after executor eth_call profit check"
        );
        return Ok(None);
    }

    candidate.expected_profit = simulated_profit;
    candidate.expected_amount_out = candidate
        .amount_in
        .checked_add(simulated_profit)
        .context("validated expected_amount_out overflow")?;
    if let Some(diagnostics) = candidate.path.diagnostics.as_mut() {
        diagnostics.modes.push("executor_eth_call".to_string());
    }
    Ok(Some(candidate))
}

fn build_execute_calldata(
    candidate: &Candidate,
    min_profit: U256,
    deadline: U256,
    settings: &Settings,
) -> Result<Vec<u8>> {
    let selector = &keccak256(
        b"executeWithOwnFunds(address,uint256,(uint8,address,address,address,address,uint24,bool,address)[],uint256,uint256)",
    )[..4];
    let mut out = Vec::new();
    out.extend_from_slice(selector);
    out.extend(encode_address(candidate.token_in));
    out.extend(encode_u256(candidate.amount_in));
    out.extend(encode_u256(U256::from(160u64)));
    out.extend(encode_u256(min_profit));
    out.extend(encode_u256(deadline));
    out.extend(encode_steps(candidate, settings)?);
    Ok(out)
}

fn encode_steps(candidate: &Candidate, settings: &Settings) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(candidate.path.steps.len())));

    for step in &candidate.path.steps {
        let router = router_for_step(step.dex, step.variant, settings)
            .with_context(|| format!("router missing for {:?} {:?}", step.dex, step.variant))?;
        out.extend(encode_u256(U256::from(executor_dex_kind(
            step.dex,
            step.variant,
        )?)));
        out.extend(encode_address(router));
        out.extend(encode_address(step.pool));
        out.extend(encode_address(step.token_in));
        out.extend(encode_address(step.token_out));
        out.extend(encode_u256(U256::from(router_fee_for_step(
            step.dex,
            step.variant,
            step.fee_bps,
            step.tick_spacing,
        )?)));
        out.extend(encode_bool(classic_stable_flag(step)));
        out.extend(encode_address(factory_for_step(step, settings)?));
    }

    Ok(out)
}

fn router_fee_for_step(
    dex: DexKind,
    variant: Option<PoolVariant>,
    fee_bps: Option<u32>,
    tick_spacing: Option<i32>,
) -> Result<u32> {
    let fee_bps = fee_bps.unwrap_or_default();
    Ok(match (dex, variant) {
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3))
        | (DexKind::UniswapV3, None)
        | (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3))
        | (DexKind::PancakeSwap, None) => {
            fee_bps.checked_mul(100).context("V3 fee bps overflow")?
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            let tick_spacing = tick_spacing.context("tick_spacing is required for Slipstream")?;
            if tick_spacing <= 0 {
                anyhow::bail!("invalid Slipstream tick_spacing {tick_spacing}");
            }
            u32::try_from(tick_spacing)?
        }
        _ => fee_bps,
    })
}

fn executor_dex_kind(dex: DexKind, variant: Option<PoolVariant>) -> Result<u8> {
    match (dex, variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            Ok(0)
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => Ok(1),
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => Ok(2),
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            Ok(2)
        }
        _ => anyhow::bail!("dex and pool variant mismatch"),
    }
}

fn classic_stable_flag(step: &SwapStep) -> bool {
    matches!(
        (step.dex, step.variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
    ) && step.stable.unwrap_or(false)
}

fn factory_for_step(step: &SwapStep, settings: &Settings) -> Result<Address> {
    if let Some(factory) = step.factory_address {
        return Ok(factory);
    }

    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            settings
                .aerodrome_pool_factory
                .context("AERODROME_POOL_FACTORY is required for Aerodrome Classic execution")
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => Ok(settings
            .aerodrome_slipstream_factory
            .unwrap_or(Address::ZERO)),
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => {
            Ok(settings.uniswap_v3_factory.unwrap_or(Address::ZERO))
        }
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            Ok(settings
                .pancake_v3_factory
                .or_else(|| PANCAKE_V3_FACTORY.parse().ok())
                .unwrap_or(Address::ZERO))
        }
        _ => Ok(Address::ZERO),
    }
}

fn router_for_step(
    dex: DexKind,
    variant: Option<PoolVariant>,
    settings: &Settings,
) -> Option<Address> {
    match (dex, variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => settings
            .aerodrome_slipstream_router
            .or_else(|| AERODROME_SLIPSTREAM_ROUTER.parse().ok()),
        (DexKind::Aerodrome, _) => settings.aerodrome_router,
        (DexKind::UniswapV3, _) => settings.uniswap_v3_router,
        (DexKind::PancakeSwap, _) => settings
            .pancake_v3_router
            .or_else(|| PANCAKE_V3_ROUTER.parse().ok()),
    }
}

fn encode_address(address: Address) -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(address.as_slice());
    out
}

fn encode_u256(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn encode_bool(value: bool) -> Vec<u8> {
    encode_u256(U256::from(u8::from(value)))
}

fn decode_uint256_result(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    U256::from_str_radix(&clean[0..64], 16).ok()
}
