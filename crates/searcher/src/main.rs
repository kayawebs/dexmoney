mod opportunity;
mod risk;
mod strategy;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::errors::ArbBotError;
use base_arb_common::types::{PoolState, PoolVariant};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, PairSearchConfigStore,
    PoolStateStore, RecorderStore, TickStateStore,
};
use std::collections::{HashMap, HashSet};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    info!("searcher initialized");
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut aggregate = SearchCycleStats::default();
    let mut last_summary = Instant::now();
    let mut runtime = SearchRuntime::new(Duration::from_secs(
        settings.searcher_full_scan_interval_secs,
    ));

    loop {
        ticker.tick().await;
        let stats = run_search_cycle(
            &mut runtime,
            &redis,
            &redis,
            &postgres,
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
                cycles = aggregate.cycles,
                total_cycle_ms = aggregate.total_cycle_ms,
                max_cycle_ms = aggregate.max_cycle_ms,
                full_scans = aggregate.full_scans,
                incremental_scans = aggregate.incremental_scans,
                changed_pools = aggregate.changed_pools,
                path_pools = aggregate.path_pools,
                total_paths = aggregate.search.total_paths,
                paths = aggregate.search.paths,
                quote_attempts = aggregate.search.quote_attempts,
                quote_successes = aggregate.search.quote_successes,
                quote_skipped = aggregate.search.quote_skipped,
                quote_skipped_missing_state = aggregate.search.quote_skipped_missing_state,
                quote_skipped_missing_ticks = aggregate.search.quote_skipped_missing_ticks,
                quote_skipped_tick_range_exhausted =
                    aggregate.search.quote_skipped_tick_range_exhausted,
                quote_skipped_state_block_gap = aggregate.search.quote_skipped_state_block_gap,
                quote_skipped_error = aggregate.search.quote_skipped_error,
                price_impact_rejected = aggregate.search.price_impact_rejected,
                min_profit_rejected = aggregate.search.min_profit_rejected,
                candidates_emitted = aggregate.search.candidates_emitted,
                risk_rejected = aggregate.risk_rejected,
                risk_expected_profit_rejected = aggregate.risk_expected_profit_rejected,
                risk_price_impact_rejected = aggregate.risk_price_impact_rejected,
                risk_path_not_whitelisted = aggregate.risk_path_not_whitelisted,
                risk_pool_state_stale = aggregate.risk_pool_state_stale,
                risk_other_rejected = aggregate.risk_other_rejected,
                opportunities_created = aggregate.opportunities_created,
                best_profit_before_impact = %aggregate.search.best_profit_before_impact,
                best_profit_rejected_by_impact = %aggregate.search.best_profit_rejected_by_impact,
                best_profit_after_impact = %aggregate.search.best_profit,
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
    cycles: u64,
    total_cycle_ms: u64,
    max_cycle_ms: u64,
    changed_pools: u64,
    path_pools: u64,
    full_scans: u64,
    incremental_scans: u64,
    risk_rejected: u64,
    risk_expected_profit_rejected: u64,
    risk_price_impact_rejected: u64,
    risk_path_not_whitelisted: u64,
    risk_pool_state_stale: u64,
    risk_other_rejected: u64,
    opportunities_created: u64,
}

impl SearchCycleStats {
    fn merge(&mut self, other: &SearchCycleStats) {
        self.search.merge(&other.search);
        self.cycles += other.cycles;
        self.total_cycle_ms += other.total_cycle_ms;
        self.max_cycle_ms = self.max_cycle_ms.max(other.max_cycle_ms);
        self.changed_pools += other.changed_pools;
        self.path_pools += other.path_pools;
        self.full_scans += other.full_scans;
        self.incremental_scans += other.incremental_scans;
        self.risk_rejected += other.risk_rejected;
        self.risk_expected_profit_rejected += other.risk_expected_profit_rejected;
        self.risk_price_impact_rejected += other.risk_price_impact_rejected;
        self.risk_path_not_whitelisted += other.risk_path_not_whitelisted;
        self.risk_pool_state_stale += other.risk_pool_state_stale;
        self.risk_other_rejected += other.risk_other_rejected;
        self.opportunities_created += other.opportunities_created;
    }

    fn record_risk_rejection(&mut self, err: &ArbBotError) {
        self.risk_rejected += 1;
        let message = err.to_string();
        if message.contains("expected_profit_below_threshold") {
            self.risk_expected_profit_rejected += 1;
        } else if message.contains("price_impact_too_high") {
            self.risk_price_impact_rejected += 1;
        } else if message.contains("path_not_whitelisted") {
            self.risk_path_not_whitelisted += 1;
        } else if message.contains("pool_state_stale") {
            self.risk_pool_state_stale += 1;
        } else {
            self.risk_other_rejected += 1;
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
struct PoolFingerprint {
    dex: base_arb_common::types::DexKind,
    variant: PoolVariant,
    token0: Address,
    token1: Address,
    fee_bps: u32,
    fee_pips: Option<u32>,
    stable: Option<bool>,
    reserve0: Option<U256>,
    reserve1: Option<U256>,
    sqrt_price_x96: Option<U256>,
    liquidity: Option<U256>,
    tick: Option<i32>,
    tick_spacing: Option<i32>,
}

impl From<&PoolState> for PoolFingerprint {
    fn from(state: &PoolState) -> Self {
        Self {
            dex: state.dex,
            variant: state.variant,
            token0: state.token0,
            token1: state.token1,
            fee_bps: state.fee_bps,
            fee_pips: state.fee_pips,
            stable: state.stable,
            reserve0: state.reserve0,
            reserve1: state.reserve1,
            sqrt_price_x96: state.sqrt_price_x96,
            liquidity: state.liquidity,
            tick: state.tick,
            tick_spacing: state.tick_spacing,
        }
    }
}

struct SearchRuntime {
    pool_fingerprints: HashMap<Address, PoolFingerprint>,
    full_scan_interval: Duration,
    last_full_scan: Option<Instant>,
}

impl SearchRuntime {
    fn new(full_scan_interval: Duration) -> Self {
        Self {
            pool_fingerprints: HashMap::new(),
            full_scan_interval,
            last_full_scan: None,
        }
    }

    fn classify_cycle(&mut self, pool_states: &[PoolState]) -> SearchCycleMode {
        let mut changed_pools = HashSet::new();
        let first_cycle = self.pool_fingerprints.is_empty();

        for state in pool_states {
            let fingerprint = PoolFingerprint::from(state);
            match self.pool_fingerprints.get(&state.pool_id.address) {
                Some(previous) if previous == &fingerprint => {}
                _ => {
                    changed_pools.insert(state.pool_id.address);
                }
            }
            self.pool_fingerprints
                .insert(state.pool_id.address, fingerprint);
        }

        let now = Instant::now();
        let full_scan_due = self
            .last_full_scan
            .map(|last| now.duration_since(last) >= self.full_scan_interval)
            .unwrap_or(true);

        if first_cycle || full_scan_due {
            self.last_full_scan = Some(now);
            SearchCycleMode::Full {
                changed_pools: changed_pools.len(),
            }
        } else {
            SearchCycleMode::Incremental { changed_pools }
        }
    }
}

enum SearchCycleMode {
    Full { changed_pools: usize },
    Incremental { changed_pools: HashSet<Address> },
}

async fn run_search_cycle<P, C, R>(
    runtime: &mut SearchRuntime,
    pool_store: &P,
    candidate_store: &C,
    recorder: &R,
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
    let cycle_started = Instant::now();
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
    let cycle_mode = runtime.classify_cycle(&pool_states);
    let changed_filter = match &cycle_mode {
        SearchCycleMode::Full { .. } => None,
        SearchCycleMode::Incremental { changed_pools } if changed_pools.is_empty() => {
            return Ok(SearchCycleStats {
                cycles: 1,
                total_cycle_ms: cycle_started.elapsed().as_millis() as u64,
                max_cycle_ms: cycle_started.elapsed().as_millis() as u64,
                incremental_scans: 1,
                ..SearchCycleStats::default()
            });
        }
        SearchCycleMode::Incremental { changed_pools } => Some(changed_pools),
    };
    let mut tick_states = Vec::new();
    let path_pools = engine.path_pool_addresses_for_changed_pools(&pool_states, changed_filter);
    for state in &pool_states {
        if !path_pools.contains(&state.pool_id.address) {
            continue;
        }
        if matches!(
            state.variant,
            base_arb_common::types::PoolVariant::AerodromeSlipstream
                | base_arb_common::types::PoolVariant::UniswapV3
                | base_arb_common::types::PoolVariant::PancakeV3
        ) {
            tick_states.extend(pool_store.get_pool_ticks(state.pool_id.address).await?);
        }
    }
    let (candidates, search_stats) = engine
        .search_with_stats_for_changed_pools(&pool_states, &tick_states, changed_filter)
        .await?;
    let mut cycle_stats = SearchCycleStats {
        search: search_stats,
        cycles: 1,
        path_pools: path_pools.len() as u64,
        ..SearchCycleStats::default()
    };
    match cycle_mode {
        SearchCycleMode::Full { changed_pools } => {
            cycle_stats.full_scans = 1;
            cycle_stats.changed_pools = changed_pools as u64;
        }
        SearchCycleMode::Incremental { changed_pools } => {
            cycle_stats.incremental_scans = 1;
            cycle_stats.changed_pools = changed_pools.len() as u64;
        }
    }

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
                recorder.record_opportunity(candidate.clone()).await?;
                candidate_store.push_candidate(candidate.clone()).await?;
                cycle_stats.opportunities_created += 1;
                debug!(candidate_id = %candidate.id, "candidate created");
            }
            Err(err) => {
                cycle_stats.record_risk_rejection(&err);
                debug!(candidate_id = %candidate.id, reason = %err, "candidate rejected");
            }
        }
    }

    let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
    cycle_stats.total_cycle_ms = elapsed_ms;
    cycle_stats.max_cycle_ms = elapsed_ms;

    Ok(cycle_stats)
}
