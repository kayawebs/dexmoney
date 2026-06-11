mod opportunity;
mod risk;
mod strategy;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::errors::ArbBotError;
use base_arb_common::types::{Candidate, DexKind, PoolState, PoolVariant, TickState};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, PairSearchConfigStore,
    PoolChangeStore, PoolStateStore, RecorderStore, TickStateStore,
};
use std::collections::{HashMap, HashSet};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

const SEARCHER_PUBLISH_BATCH_PATHS: usize = 32;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    info!("searcher initialized");
    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut aggregate = SearchCycleStats::default();
    let mut last_summary = Instant::now();
    let mut runtime = SearchRuntime::default();

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
                state_load_ms = aggregate.state_load_ms,
                tick_load_ms = aggregate.tick_load_ms,
                tick_cache_hits = aggregate.tick_cache_hits,
                tick_cache_misses = aggregate.tick_cache_misses,
                tick_cache_refreshes = aggregate.tick_cache_refreshes,
                path_build_ms = aggregate.path_build_ms,
                quote_ms = aggregate.quote_ms,
                publish_ms = aggregate.publish_ms,
                idle_cycles = aggregate.idle_cycles,
                changed_pool_scans = aggregate.changed_pool_scans,
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
                candidates_coalesced = aggregate.candidates_coalesced,
                stale_publish_rejected = aggregate.stale_publish_rejected,
                dynamic_multihop_paths = aggregate.search.dynamic_multihop_paths,
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
    state_load_ms: u64,
    tick_load_ms: u64,
    tick_cache_hits: u64,
    tick_cache_misses: u64,
    tick_cache_refreshes: u64,
    path_build_ms: u64,
    quote_ms: u64,
    publish_ms: u64,
    idle_cycles: u64,
    changed_pool_scans: u64,
    changed_pools: u64,
    path_pools: u64,
    risk_rejected: u64,
    risk_expected_profit_rejected: u64,
    risk_price_impact_rejected: u64,
    risk_path_not_whitelisted: u64,
    risk_pool_state_stale: u64,
    risk_other_rejected: u64,
    candidates_coalesced: u64,
    stale_publish_rejected: u64,
    opportunities_created: u64,
}

impl SearchCycleStats {
    fn merge(&mut self, other: &SearchCycleStats) {
        self.search.merge(&other.search);
        self.cycles += other.cycles;
        self.total_cycle_ms += other.total_cycle_ms;
        self.max_cycle_ms = self.max_cycle_ms.max(other.max_cycle_ms);
        self.state_load_ms += other.state_load_ms;
        self.tick_load_ms += other.tick_load_ms;
        self.tick_cache_hits += other.tick_cache_hits;
        self.tick_cache_misses += other.tick_cache_misses;
        self.tick_cache_refreshes += other.tick_cache_refreshes;
        self.path_build_ms += other.path_build_ms;
        self.quote_ms += other.quote_ms;
        self.publish_ms += other.publish_ms;
        self.idle_cycles += other.idle_cycles;
        self.changed_pool_scans += other.changed_pool_scans;
        self.changed_pools += other.changed_pools;
        self.path_pools += other.path_pools;
        self.risk_rejected += other.risk_rejected;
        self.risk_expected_profit_rejected += other.risk_expected_profit_rejected;
        self.risk_price_impact_rejected += other.risk_price_impact_rejected;
        self.risk_path_not_whitelisted += other.risk_path_not_whitelisted;
        self.risk_pool_state_stale += other.risk_pool_state_stale;
        self.risk_other_rejected += other.risk_other_rejected;
        self.candidates_coalesced += other.candidates_coalesced;
        self.stale_publish_rejected += other.stale_publish_rejected;
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

#[derive(Default)]
struct SearchRuntime {
    pool_states: HashMap<Address, PoolState>,
    tick_states: HashMap<Address, Vec<TickState>>,
    pool_topology: HashMap<Address, PoolTopology>,
    path_index: Option<strategy::PathIndex>,
    graph_snapshot: Option<strategy::GraphSnapshot>,
    bootstrapped: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PoolTopology {
    dex: DexKind,
    variant: PoolVariant,
    token0: Address,
    token1: Address,
    fee_bps: u32,
    fee_pips: Option<u32>,
    stable: Option<bool>,
    tick_spacing: Option<i32>,
}

impl From<&PoolState> for PoolTopology {
    fn from(state: &PoolState) -> Self {
        Self {
            dex: state.dex,
            variant: state.variant,
            token0: state.token0,
            token1: state.token1,
            fee_bps: state.fee_bps,
            fee_pips: state.fee_pips,
            stable: state.stable,
            tick_spacing: state.tick_spacing,
        }
    }
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
    P: PoolStateStore + PoolChangeStore + TickStateStore,
    C: CandidateStore,
    R: RecorderStore + PairSearchConfigStore,
{
    let cycle_started = Instant::now();
    if !runtime.bootstrapped {
        for state in pool_store.all_pool_states().await? {
            runtime
                .pool_topology
                .insert(state.pool_id.address, PoolTopology::from(&state));
            runtime.pool_states.insert(state.pool_id.address, state);
        }
        runtime.bootstrapped = true;
        info!(
            pools = runtime.pool_states.len(),
            "searcher pool state cache bootstrapped"
        );
    }

    let changed_pools = pool_store
        .drain_changed_pools()
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    if changed_pools.is_empty() {
        return Ok(SearchCycleStats {
            cycles: 1,
            total_cycle_ms: cycle_started.elapsed().as_millis() as u64,
            max_cycle_ms: cycle_started.elapsed().as_millis() as u64,
            idle_cycles: 1,
            ..SearchCycleStats::default()
        });
    }

    let engine = strategy::engine_from_settings(
        settings,
        candidate_ttl_ms,
        max_price_impact_bps,
        strategy::usdc_to_units(min_expected_profit_usdc),
        recorder.enabled_pair_search_configs().await?,
    )?;

    let state_load_started = Instant::now();
    let mut rebuild_path_index = runtime.path_index.is_none();
    for pool in &changed_pools {
        match pool_store.get_pool_state(*pool).await? {
            Some(state) => {
                let topology = PoolTopology::from(&state);
                if runtime.pool_topology.get(pool) != Some(&topology) {
                    rebuild_path_index = true;
                    runtime.pool_topology.insert(*pool, topology);
                }
                runtime.pool_states.insert(*pool, state);
            }
            None => {
                if runtime.pool_topology.remove(pool).is_some() {
                    rebuild_path_index = true;
                }
                runtime.pool_states.remove(pool);
                runtime.tick_states.remove(pool);
            }
        }
    }
    let state_load_ms = state_load_started.elapsed().as_millis() as u64;

    let pool_states = runtime.pool_states.values().cloned().collect::<Vec<_>>();
    if pool_states.is_empty() {
        debug!("no pool states available in searcher cache");
        return Ok(SearchCycleStats::default());
    }
    let latest_known_block = pool_states
        .iter()
        .map(|state| state.effective_valid_through_block())
        .max()
        .unwrap_or(0);
    if rebuild_path_index {
        runtime.path_index = Some(engine.build_path_index(&pool_states));
        runtime.graph_snapshot = Some(engine.build_graph_snapshot(&pool_states));
        info!(
            total_paths = runtime
                .path_index
                .as_ref()
                .map(|index| index.total_paths())
                .unwrap_or_default(),
            multihop_enabled = engine.multihop_enabled,
            "searcher path index rebuilt"
        );
    }
    let Some(path_index) = runtime.path_index.as_ref() else {
        return Ok(SearchCycleStats::default());
    };
    let Some(graph_snapshot) = runtime.graph_snapshot.as_ref() else {
        return Ok(SearchCycleStats::default());
    };
    let path_build_started = Instant::now();
    let dynamic_paths =
        engine.dynamic_multihop_paths_for_changed_pools(graph_snapshot, &changed_pools);
    let mut path_pools = engine.path_pool_addresses_for_path_index(path_index, &changed_pools);
    path_pools.extend(engine.path_pool_addresses_for_search_paths(&dynamic_paths));
    let search_paths =
        engine.search_paths_for_path_index(path_index, &changed_pools, &dynamic_paths);
    let path_build_ms = path_build_started.elapsed().as_millis() as u64;

    let tick_load_started = Instant::now();
    let mut tick_states = Vec::new();
    let mut tick_cache_hits = 0u64;
    let mut tick_cache_misses = 0u64;
    let mut tick_cache_refreshes = 0u64;
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
            let pool = state.pool_id.address;
            let should_refresh =
                changed_pools.contains(&pool) || !runtime.tick_states.contains_key(&pool);
            if should_refresh {
                let loaded_ticks = pool_store.get_pool_ticks(pool).await?;
                tick_cache_misses += 1;
                tick_cache_refreshes += 1;
                runtime.tick_states.insert(pool, loaded_ticks);
            } else {
                tick_cache_hits += 1;
            }
            if let Some(cached_ticks) = runtime.tick_states.get(&pool) {
                tick_states.extend(cached_ticks.iter().cloned());
            }
        }
    }
    let tick_load_ms = tick_load_started.elapsed().as_millis() as u64;
    let mut cycle_stats = SearchCycleStats {
        cycles: 1,
        state_load_ms,
        tick_load_ms,
        tick_cache_hits,
        tick_cache_misses,
        tick_cache_refreshes,
        path_build_ms,
        path_pools: path_pools.len() as u64,
        changed_pool_scans: 1,
        changed_pools: changed_pools.len() as u64,
        ..SearchCycleStats::default()
    };
    cycle_stats.search.total_paths = path_index.total_paths();
    cycle_stats.search.dynamic_multihop_paths = dynamic_paths.len() as u64;

    for path_batch in search_paths.chunks(SEARCHER_PUBLISH_BATCH_PATHS) {
        let quote_started = Instant::now();
        let (candidates, mut search_stats) = engine
            .search_with_stats_for_paths(&pool_states, &tick_states, path_batch)
            .await?;
        cycle_stats.quote_ms += quote_started.elapsed().as_millis() as u64;
        search_stats.total_paths = 0;
        cycle_stats.search.merge(&search_stats);
        let publish_started = Instant::now();
        publish_candidates(
            candidate_store,
            recorder,
            &mut cycle_stats,
            &engine,
            &pool_states,
            max_pool_state_age_ms,
            max_price_impact_bps,
            latest_known_block,
            settings.execution_max_candidate_lag_blocks.max(1),
            candidates,
        )
        .await?;
        cycle_stats.publish_ms += publish_started.elapsed().as_millis() as u64;
    }

    let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
    cycle_stats.total_cycle_ms = elapsed_ms;
    cycle_stats.max_cycle_ms = elapsed_ms;

    Ok(cycle_stats)
}

async fn publish_candidates<C, R>(
    candidate_store: &C,
    recorder: &R,
    cycle_stats: &mut SearchCycleStats,
    engine: &strategy::SearchEngine,
    pool_states: &[PoolState],
    max_pool_state_age_ms: i64,
    max_price_impact_bps: u64,
    latest_known_block: u64,
    max_candidate_lag_blocks: u64,
    candidates: Vec<base_arb_common::types::Candidate>,
) -> Result<()>
where
    C: CandidateStore,
    R: RecorderStore,
{
    let original_candidate_count = candidates.len();
    let candidates = coalesce_candidates(candidates);
    cycle_stats.candidates_coalesced +=
        original_candidate_count.saturating_sub(candidates.len()) as u64;
    for candidate in candidates {
        let candidate_lag = latest_known_block.saturating_sub(candidate.block_number);
        if candidate_lag > max_candidate_lag_blocks {
            cycle_stats.stale_publish_rejected += 1;
            debug!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                candidate_block = candidate.block_number,
                latest_known_block,
                candidate_lag,
                max_candidate_lag_blocks,
                "candidate rejected before publish because quote block is stale"
            );
            continue;
        }
        debug!(candidate_id = %candidate.id, "quote generated");
        match risk::validate_candidate(
            &candidate,
            pool_states,
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
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CandidateExecutorScope {
    TwoHop,
    MultiHop,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CandidateCoalesceKey {
    block_number: u64,
    token_in: Address,
    amount_in: U256,
    executor_scope: CandidateExecutorScope,
}

fn coalesce_candidates(candidates: Vec<Candidate>) -> Vec<Candidate> {
    let mut best_by_key: HashMap<CandidateCoalesceKey, Candidate> = HashMap::new();
    for candidate in candidates {
        let key = CandidateCoalesceKey {
            block_number: candidate.block_number,
            token_in: candidate.token_in,
            amount_in: candidate.amount_in,
            executor_scope: candidate_executor_scope(&candidate),
        };
        match best_by_key.get_mut(&key) {
            Some(existing) if existing.expected_profit >= candidate.expected_profit => {}
            Some(existing) => {
                *existing = candidate;
            }
            None => {
                best_by_key.insert(key, candidate);
            }
        }
    }

    let mut out = best_by_key.into_values().collect::<Vec<_>>();
    out.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));
    out
}

fn candidate_executor_scope(candidate: &Candidate) -> CandidateExecutorScope {
    if candidate.path.steps.len() == 2 {
        CandidateExecutorScope::TwoHop
    } else {
        CandidateExecutorScope::MultiHop
    }
}
