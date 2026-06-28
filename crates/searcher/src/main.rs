mod opportunity;
mod risk;
mod strategy;

use alloy_primitives::{Address, U256};
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::constants::{
    AERODROME_CLASSIC_FACTORY, AERODROME_SLIPSTREAM_FACTORIES, PANCAKE_V3_FACTORY,
    UNISWAP_V3_FACTORY,
};
use base_arb_common::errors::ArbBotError;
use base_arb_common::types::{
    Candidate, DexKind, PoolState, PoolVariant, TickState, TokenPairSearchConfig,
};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, CurrentBlockStore,
    PairSearchConfigStore, PoolChangeStore, PoolStateStore, RecorderStore, TickChangeStore,
    TickRepairStore, TickStateStore,
};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const SEARCHER_PUBLISH_BATCH_PATHS: usize = 256;
const ACTIVE_POOL_SWEEP_MIN_MS: u64 = 100;
const ACTIVE_POOL_SWEEP_MAX_MS: u64 = 1_000;
const TICK_CACHE_MAX_AGE_MS: u64 = 30_000;
const OPPORTUNITY_RECORD_QUEUE_CAPACITY: usize = 1_024;
const OPPORTUNITY_RECORD_FLUSH_MS: u64 = 100;
const OPPORTUNITY_RECORD_MAX_BATCH: usize = 512;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let opportunity_recorder = OpportunityRecordQueue::spawn(postgres.clone());

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
            &opportunity_recorder,
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
                config_load_ms = aggregate.config_load_ms,
                state_load_ms = aggregate.state_load_ms,
                state_load_pools = aggregate.state_load_pools,
                tick_load_ms = aggregate.tick_load_ms,
                tick_cache_hits = aggregate.tick_cache_hits,
                tick_cache_misses = aggregate.tick_cache_misses,
                tick_cache_refreshes = aggregate.tick_cache_refreshes,
                path_index_rebuild_ms = aggregate.path_index_rebuild_ms,
                path_build_ms = aggregate.path_build_ms,
                quote_ms = aggregate.quote_ms,
                publish_ms = aggregate.publish_ms,
                idle_cycles = aggregate.idle_cycles,
                changed_pool_scans = aggregate.changed_pool_scans,
                changed_pools = aggregate.changed_pools,
                active_pool_sweeps = aggregate.active_pool_sweeps,
                active_pools = aggregate.active_pools,
                path_pools = aggregate.path_pools,
                latest_chain_block = aggregate.latest_chain_block,
                latest_pool_state_block = aggregate.latest_pool_state_block,
                total_paths = aggregate.search.total_paths,
                paths = aggregate.search.paths,
                quote_attempts = aggregate.search.quote_attempts,
                quote_successes = aggregate.search.quote_successes,
                quote_skipped = aggregate.search.quote_skipped,
                quote_skipped_missing_state = aggregate.search.quote_skipped_missing_state,
                quote_skipped_missing_ticks = aggregate.search.quote_skipped_missing_ticks,
                quote_skipped_tick_range_exhausted =
                    aggregate.search.quote_skipped_tick_range_exhausted,
                quote_skipped_error = aggregate.search.quote_skipped_error,
                price_impact_rejected = aggregate.search.price_impact_rejected,
                price_impact_shadow_pass_100_bps =
                    aggregate.search.price_impact_shadow_pass_100_bps,
                price_impact_shadow_pass_150_bps =
                    aggregate.search.price_impact_shadow_pass_150_bps,
                price_impact_shadow_pass_300_bps =
                    aggregate.search.price_impact_shadow_pass_300_bps,
                price_impact_shadow_pass_500_bps =
                    aggregate.search.price_impact_shadow_pass_500_bps,
                quote_model_edge_rejected = aggregate.search.quote_model_edge_rejected,
                min_profit_rejected = aggregate.search.min_profit_rejected,
                candidates_emitted = aggregate.search.candidates_emitted,
                candidates_coalesced = aggregate.candidates_coalesced,
                inactive_pool_filtered_paths = aggregate.inactive_pool_filtered_paths,
                tick_missing_filtered_paths = aggregate.tick_missing_filtered_paths,
                tick_repair_queued_pools = aggregate.tick_repair_queued_pools,
                opportunity_record_queue_dropped = aggregate.opportunity_record_queue_dropped,
                dynamic_multihop_paths = aggregate.search.dynamic_multihop_paths,
                dynamic_multihop_anchors = aggregate.search.dynamic_multihop_anchors,
                dynamic_multihop_changed_edges = aggregate.search.dynamic_multihop_changed_edges,
                dynamic_multihop_prefix_empty = aggregate.search.dynamic_multihop_prefix_empty,
                dynamic_multihop_suffix_empty = aggregate.search.dynamic_multihop_suffix_empty,
                dynamic_multihop_invalid_cycle = aggregate.search.dynamic_multihop_invalid_cycle,
                dynamic_multihop_duplicate_cycle =
                    aggregate.search.dynamic_multihop_duplicate_cycle,
                dynamic_multihop_rough_quote_failed =
                    aggregate.search.dynamic_multihop_rough_quote_failed,
                dynamic_multihop_rough_missing_reserves =
                    aggregate.search.dynamic_multihop_rough_missing_reserves,
                dynamic_multihop_rough_token_mismatch =
                    aggregate.search.dynamic_multihop_rough_token_mismatch,
                dynamic_multihop_rough_missing_decimals =
                    aggregate.search.dynamic_multihop_rough_missing_decimals,
                dynamic_multihop_rough_stable_quote_failed =
                    aggregate.search.dynamic_multihop_rough_stable_quote_failed,
                dynamic_multihop_rough_v2_quote_failed =
                    aggregate.search.dynamic_multihop_rough_v2_quote_failed,
                dynamic_multihop_rough_balancer_quote_failed =
                    aggregate.search.dynamic_multihop_rough_balancer_quote_failed,
                dynamic_multihop_rough_missing_v3_state =
                    aggregate.search.dynamic_multihop_rough_missing_v3_state,
                dynamic_multihop_rough_v3_spot_quote_failed =
                    aggregate.search.dynamic_multihop_rough_v3_spot_quote_failed,
                dynamic_multihop_rough_v3_spot_overflow =
                    aggregate.search.dynamic_multihop_rough_v3_spot_overflow,
                dynamic_multihop_rough_unsupported_pool =
                    aggregate.search.dynamic_multihop_rough_unsupported_pool,
                dynamic_multihop_rough_zero_output =
                    aggregate.search.dynamic_multihop_rough_zero_output,
                dynamic_multihop_rough_quote_included =
                    aggregate.search.dynamic_multihop_rough_quote_included,
                dynamic_multihop_rough_quote_dropped =
                    aggregate.search.dynamic_multihop_rough_quote_dropped,
                dynamic_multihop_rough_profit_below_min =
                    aggregate.search.dynamic_multihop_rough_profit_below_min,
                dynamic_multihop_candidate_cap_hit =
                    aggregate.search.dynamic_multihop_candidate_cap_hit,
                dynamic_multihop_priority_edges =
                    aggregate.search.dynamic_multihop_priority_edges,
                risk_rejected = aggregate.risk_rejected,
                risk_expected_profit_rejected = aggregate.risk_expected_profit_rejected,
                risk_price_impact_rejected = aggregate.risk_price_impact_rejected,
                risk_path_not_whitelisted = aggregate.risk_path_not_whitelisted,
                risk_pool_state_stale = aggregate.risk_pool_state_stale,
                risk_other_rejected = aggregate.risk_other_rejected,
                opportunities_created = aggregate.opportunities_created,
                best_profit_before_impact = %aggregate.search.best_profit_before_impact,
                best_profit_rejected_by_impact = %aggregate.search.best_profit_rejected_by_impact,
                best_profit_shadow_pass_100_bps = %aggregate.search.best_profit_shadow_pass_100_bps,
                best_profit_shadow_pass_150_bps = %aggregate.search.best_profit_shadow_pass_150_bps,
                best_profit_shadow_pass_300_bps = %aggregate.search.best_profit_shadow_pass_300_bps,
                best_profit_shadow_pass_500_bps = %aggregate.search.best_profit_shadow_pass_500_bps,
                best_profit_rejected_by_model_edge = %aggregate.search.best_profit_rejected_by_model_edge,
                best_profit_after_impact = %aggregate.search.best_profit,
                top_min_profit_rejected = %aggregate.search.top_min_profit_rejected_summary(),
                top_price_impact_rejected = %aggregate.search.top_price_impact_rejected_summary(),
                top_quote_skipped = %aggregate.search.top_quote_skipped_summary(),
                top_rough_quote_failures = %aggregate.search.top_rough_quote_failure_summary(),
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
    config_load_ms: u64,
    state_load_ms: u64,
    state_load_pools: u64,
    tick_load_ms: u64,
    tick_cache_hits: u64,
    tick_cache_misses: u64,
    tick_cache_refreshes: u64,
    path_index_rebuild_ms: u64,
    path_build_ms: u64,
    quote_ms: u64,
    publish_ms: u64,
    idle_cycles: u64,
    changed_pool_scans: u64,
    changed_pools: u64,
    active_pool_sweeps: u64,
    active_pools: u64,
    path_pools: u64,
    latest_chain_block: u64,
    latest_pool_state_block: u64,
    risk_rejected: u64,
    risk_expected_profit_rejected: u64,
    risk_price_impact_rejected: u64,
    risk_path_not_whitelisted: u64,
    risk_pool_state_stale: u64,
    risk_other_rejected: u64,
    candidates_coalesced: u64,
    inactive_pool_filtered_paths: u64,
    tick_missing_filtered_paths: u64,
    tick_repair_queued_pools: u64,
    opportunity_record_queue_dropped: u64,
    opportunities_created: u64,
}

impl SearchCycleStats {
    fn merge(&mut self, other: &SearchCycleStats) {
        self.search.merge(&other.search);
        self.cycles += other.cycles;
        self.total_cycle_ms += other.total_cycle_ms;
        self.max_cycle_ms = self.max_cycle_ms.max(other.max_cycle_ms);
        self.config_load_ms += other.config_load_ms;
        self.state_load_ms += other.state_load_ms;
        self.state_load_pools += other.state_load_pools;
        self.tick_load_ms += other.tick_load_ms;
        self.tick_cache_hits += other.tick_cache_hits;
        self.tick_cache_misses += other.tick_cache_misses;
        self.tick_cache_refreshes += other.tick_cache_refreshes;
        self.path_index_rebuild_ms += other.path_index_rebuild_ms;
        self.path_build_ms += other.path_build_ms;
        self.quote_ms += other.quote_ms;
        self.publish_ms += other.publish_ms;
        self.idle_cycles += other.idle_cycles;
        self.changed_pool_scans += other.changed_pool_scans;
        self.changed_pools += other.changed_pools;
        self.active_pool_sweeps += other.active_pool_sweeps;
        self.active_pools = self.active_pools.max(other.active_pools);
        self.path_pools += other.path_pools;
        self.latest_chain_block = self.latest_chain_block.max(other.latest_chain_block);
        self.latest_pool_state_block = self
            .latest_pool_state_block
            .max(other.latest_pool_state_block);
        self.risk_rejected += other.risk_rejected;
        self.risk_expected_profit_rejected += other.risk_expected_profit_rejected;
        self.risk_price_impact_rejected += other.risk_price_impact_rejected;
        self.risk_path_not_whitelisted += other.risk_path_not_whitelisted;
        self.risk_pool_state_stale += other.risk_pool_state_stale;
        self.risk_other_rejected += other.risk_other_rejected;
        self.candidates_coalesced += other.candidates_coalesced;
        self.inactive_pool_filtered_paths += other.inactive_pool_filtered_paths;
        self.tick_missing_filtered_paths += other.tick_missing_filtered_paths;
        self.tick_repair_queued_pools += other.tick_repair_queued_pools;
        self.opportunity_record_queue_dropped += other.opportunity_record_queue_dropped;
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

#[derive(Clone)]
struct OpportunityRecordQueue {
    sender: mpsc::Sender<Vec<Candidate>>,
}

impl OpportunityRecordQueue {
    fn spawn(store: PostgresStore) -> Self {
        let (sender, mut receiver) =
            mpsc::channel::<Vec<Candidate>>(OPPORTUNITY_RECORD_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut pending = Vec::new();
            let mut flush_interval = interval(Duration::from_millis(OPPORTUNITY_RECORD_FLUSH_MS));
            flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    maybe_candidates = receiver.recv() => {
                        let Some(mut candidates) = maybe_candidates else {
                            flush_opportunity_records(&store, &mut pending).await;
                            break;
                        };
                        pending.append(&mut candidates);
                        if pending.len() >= OPPORTUNITY_RECORD_MAX_BATCH {
                            flush_opportunity_records(&store, &mut pending).await;
                        }
                    }
                    _ = flush_interval.tick(), if !pending.is_empty() => {
                        flush_opportunity_records(&store, &mut pending).await;
                    }
                }
            }
        });
        Self { sender }
    }

    fn enqueue(&self, candidates: Vec<Candidate>) -> usize {
        if candidates.is_empty() {
            return 0;
        }
        let count = candidates.len();
        match self.sender.try_send(candidates) {
            Ok(()) => 0,
            Err(mpsc::error::TrySendError::Full(candidates)) => {
                warn!(
                    dropped = candidates.len(),
                    "opportunity record queue full; dropping diagnostic records"
                );
                candidates.len()
            }
            Err(mpsc::error::TrySendError::Closed(candidates)) => {
                warn!(
                    dropped = candidates.len(),
                    "opportunity record queue closed; dropping diagnostic records"
                );
                candidates.len()
            }
        }
        .min(count)
    }
}

async fn flush_opportunity_records(store: &PostgresStore, pending: &mut Vec<Candidate>) {
    if pending.is_empty() {
        return;
    }
    let candidates = std::mem::take(pending);
    let count = candidates.len();
    if let Err(err) = store.record_opportunities(candidates).await {
        warn!(
            count,
            error = %err,
            "failed to record opportunities from background queue"
        );
    }
}

#[derive(Default)]
struct SearchRuntime {
    pool_states: HashMap<Address, PoolState>,
    tick_states: HashMap<Address, Vec<TickState>>,
    tick_states_loaded_at: HashMap<Address, Instant>,
    active_pool_addresses: HashSet<Address>,
    pool_topology: HashMap<Address, PoolTopology>,
    latest_pool_state_block: u64,
    engine: Option<strategy::SearchEngine>,
    pair_configs: Vec<TokenPairSearchConfig>,
    last_config_refresh: Option<Instant>,
    last_active_pool_sweep: Option<Instant>,
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
    config_store: &R,
    opportunity_recorder: &OpportunityRecordQueue,
    settings: &Settings,
    candidate_ttl_ms: i64,
    max_pool_state_age_ms: i64,
    max_price_impact_bps: u64,
    min_expected_profit_usdc: f64,
) -> Result<SearchCycleStats>
where
    P: PoolStateStore
        + PoolChangeStore
        + CurrentBlockStore
        + TickChangeStore
        + TickRepairStore
        + TickStateStore,
    C: CandidateStore,
    R: PairSearchConfigStore,
{
    let cycle_started = Instant::now();
    if !runtime.bootstrapped {
        for state in pool_store.all_pool_states().await? {
            runtime.latest_pool_state_block =
                runtime.latest_pool_state_block.max(state.block_number);
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

    let mut changed_pools = pool_store
        .drain_changed_pools()
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    let tick_changed_pools = pool_store
        .drain_tick_changed_pools()
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    changed_pools.extend(tick_changed_pools.iter().copied());
    if changed_pools.is_empty() {
        return Ok(SearchCycleStats {
            cycles: 1,
            total_cycle_ms: cycle_started.elapsed().as_millis() as u64,
            max_cycle_ms: cycle_started.elapsed().as_millis() as u64,
            idle_cycles: 1,
            ..SearchCycleStats::default()
        });
    }

    let config_load_started = Instant::now();
    let config_refresh_interval = Duration::from_secs(settings.searcher_config_refresh_secs.max(1));
    let config_refresh_due = runtime
        .last_config_refresh
        .is_none_or(|last_refresh| last_refresh.elapsed() >= config_refresh_interval);
    let mut config_changed = false;
    if runtime.engine.is_none() || config_refresh_due {
        let pair_configs = config_store.enabled_pair_search_configs().await?;
        if runtime.engine.is_none() || runtime.pair_configs != pair_configs {
            runtime.engine = Some(strategy::engine_from_settings(
                settings,
                candidate_ttl_ms,
                max_price_impact_bps,
                strategy::usdc_to_units(min_expected_profit_usdc),
                pair_configs.clone(),
            )?);
            runtime.pair_configs = pair_configs;
            runtime.path_index = None;
            runtime.graph_snapshot = None;
            config_changed = true;
            info!(
                pair_configs = runtime.pair_configs.len(),
                "searcher config refreshed"
            );
        }
        runtime.last_config_refresh = Some(Instant::now());
    }
    let config_load_ms = config_load_started.elapsed().as_millis() as u64;
    if runtime.engine.is_none() {
        debug!("searcher config unavailable");
        return Ok(SearchCycleStats::default());
    }

    let state_load_started = Instant::now();
    let now = Utc::now();
    let mut rebuild_path_index =
        config_changed || runtime.path_index.is_none() || runtime.graph_snapshot.is_none();
    let state_load_addresses = changed_pools
        .iter()
        .copied()
        .filter(|pool| {
            !tick_changed_pools.contains(pool) || !runtime.pool_states.contains_key(pool)
        })
        .collect::<Vec<_>>();
    for (pool, state) in pool_store.get_pool_states(&state_load_addresses).await? {
        match state {
            Some(state) => {
                let topology = PoolTopology::from(&state);
                let was_active = runtime.active_pool_addresses.contains(&pool);
                let quote_ready = is_pool_state_quote_ready(&state, settings);
                if quote_ready
                    && (!state.is_stale(now, max_pool_state_age_ms)
                        || changed_pools.contains(&pool))
                {
                    runtime.active_pool_addresses.insert(pool);
                } else {
                    runtime.active_pool_addresses.remove(&pool);
                }
                if was_active != runtime.active_pool_addresses.contains(&pool) {
                    rebuild_path_index = true;
                }
                if runtime.pool_topology.get(&pool) != Some(&topology) {
                    rebuild_path_index = true;
                    runtime.pool_topology.insert(pool, topology);
                }
                runtime.latest_pool_state_block =
                    runtime.latest_pool_state_block.max(state.block_number);
                runtime.pool_states.insert(pool, state);
            }
            None => {
                if runtime.pool_topology.remove(&pool).is_some() {
                    rebuild_path_index = true;
                }
                if runtime
                    .pool_states
                    .remove(&pool)
                    .is_some_and(|state| state.block_number == runtime.latest_pool_state_block)
                {
                    runtime.latest_pool_state_block = latest_pool_state_block(&runtime.pool_states);
                }
                runtime.tick_states.remove(&pool);
                runtime.tick_states_loaded_at.remove(&pool);
                if runtime.active_pool_addresses.remove(&pool) {
                    rebuild_path_index = true;
                }
            }
        }
    }
    let state_load_ms = state_load_started.elapsed().as_millis() as u64;

    if runtime.pool_states.is_empty() {
        debug!("no pool states available in searcher cache");
        return Ok(SearchCycleStats::default());
    }
    let latest_pool_state_block = runtime.latest_pool_state_block;
    let Some(latest_known_block) = pool_store.get_current_block().await? else {
        debug!("current block not available from market-data");
        return Ok(SearchCycleStats::default());
    };
    let max_candidate_lag_blocks = settings.execution_max_candidate_lag_blocks.max(1);
    let active_pool_refresh =
        refresh_active_pool_cache_if_due(runtime, now, max_pool_state_age_ms, settings);
    if active_pool_refresh.changed {
        rebuild_path_index = true;
    }
    if force_changed_quote_ready_pools_active(runtime, &changed_pools, settings) {
        rebuild_path_index = true;
    }
    if runtime.active_pool_addresses.is_empty() {
        debug!(
            latest_known_block,
            latest_pool_state_block,
            max_pool_state_age_ms,
            "no active pool states available in searcher cache"
        );
        return Ok(SearchCycleStats::default());
    }
    let mut path_index_rebuild_ms = 0;
    if rebuild_path_index {
        let rebuild_started = Instant::now();
        let engine = runtime
            .engine
            .as_ref()
            .expect("searcher engine checked above");
        let pool_states = active_pool_states(runtime);
        runtime.path_index = Some(engine.build_path_index(&pool_states));
        runtime.graph_snapshot = Some(engine.build_graph_snapshot(pool_states));
        path_index_rebuild_ms = rebuild_started.elapsed().as_millis() as u64;
        info!(
            active_pools = runtime.active_pool_addresses.len(),
            total_paths = runtime
                .path_index
                .as_ref()
                .map(|index| index.total_paths())
                .unwrap_or_default(),
            rebuild_ms = path_index_rebuild_ms,
            multihop_enabled = engine.multihop_enabled,
            "searcher path index rebuilt"
        );
    }
    let path_build_started = Instant::now();
    let Some(path_index) = runtime.path_index.as_ref() else {
        return Ok(SearchCycleStats::default());
    };
    let Some(graph_snapshot) = runtime.graph_snapshot.as_ref() else {
        return Ok(SearchCycleStats::default());
    };
    let engine = runtime
        .engine
        .as_ref()
        .expect("searcher engine checked above");
    let (dynamic_paths, dynamic_stats) =
        engine.dynamic_multihop_paths_for_changed_pools(graph_snapshot, &changed_pools);
    let selected_paths = engine.select_search_paths_for_path_index(
        path_index,
        &changed_pools,
        &dynamic_paths,
        &runtime.active_pool_addresses,
    );
    let path_build_ms = path_build_started.elapsed().as_millis() as u64;

    let mut cycle_stats = SearchCycleStats {
        cycles: 1,
        config_load_ms,
        state_load_ms,
        state_load_pools: state_load_addresses.len() as u64,
        path_index_rebuild_ms,
        path_build_ms,
        path_pools: selected_paths.path_pools.len() as u64,
        latest_chain_block: latest_known_block,
        latest_pool_state_block,
        inactive_pool_filtered_paths: selected_paths.inactive_pool_filtered_paths as u64,
        active_pool_sweeps: u64::from(active_pool_refresh.swept),
        active_pools: runtime.active_pool_addresses.len() as u64,
        changed_pool_scans: 1,
        changed_pools: changed_pools.len() as u64,
        ..SearchCycleStats::default()
    };
    cycle_stats.search.total_paths = path_index.total_paths();
    cycle_stats.search.merge(&dynamic_stats);
    let mut refreshed_tick_pools = HashSet::new();
    let tick_load_started = Instant::now();
    let tick_load_stats = load_tick_states_for_pools(
        &runtime.pool_states,
        &mut runtime.tick_states,
        &mut runtime.tick_states_loaded_at,
        pool_store,
        &tick_changed_pools,
        &mut refreshed_tick_pools,
        &selected_paths.path_pools,
    )
    .await?;
    cycle_stats.tick_load_ms += tick_load_started.elapsed().as_millis() as u64;
    cycle_stats.tick_cache_hits += tick_load_stats.cache_hits;
    cycle_stats.tick_cache_misses += tick_load_stats.cache_misses;
    cycle_stats.tick_cache_refreshes += tick_load_stats.cache_refreshes;

    let missing_tick_pools = missing_tick_pools_for_paths(
        &runtime.pool_states,
        &runtime.tick_states,
        &selected_paths.path_pools,
    );
    let quote_context =
        strategy::QuoteContext::from_pool_map(&runtime.pool_states, &runtime.tick_states);

    let mut path_batch = Vec::with_capacity(SEARCHER_PUBLISH_BATCH_PATHS);
    for path in selected_paths.paths.iter().copied() {
        if !path.all_pools_not_in(&missing_tick_pools) {
            cycle_stats.tick_missing_filtered_paths += 1;
            continue;
        }
        path_batch.push(path);
        if path_batch.len() >= SEARCHER_PUBLISH_BATCH_PATHS {
            quote_and_publish_path_batch(
                candidate_store,
                pool_store,
                opportunity_recorder,
                &mut cycle_stats,
                engine,
                &quote_context,
                &runtime.active_pool_addresses,
                max_pool_state_age_ms,
                max_price_impact_bps,
                latest_known_block,
                max_candidate_lag_blocks,
                &path_batch,
            )
            .await?;
            path_batch.clear();
        }
    }
    if !path_batch.is_empty() {
        quote_and_publish_path_batch(
            candidate_store,
            pool_store,
            opportunity_recorder,
            &mut cycle_stats,
            engine,
            &quote_context,
            &runtime.active_pool_addresses,
            max_pool_state_age_ms,
            max_price_impact_bps,
            latest_known_block,
            max_candidate_lag_blocks,
            &path_batch,
        )
        .await?;
    }

    let elapsed_ms = cycle_started.elapsed().as_millis() as u64;
    cycle_stats.total_cycle_ms = elapsed_ms;
    cycle_stats.max_cycle_ms = elapsed_ms;

    Ok(cycle_stats)
}

fn active_pool_addresses<'a>(
    pool_states: impl Iterator<Item = &'a PoolState>,
    now: DateTime<Utc>,
    max_pool_state_age_ms: i64,
    settings: &Settings,
) -> HashSet<Address> {
    pool_states
        .filter(|state| is_pool_state_active(state, now, max_pool_state_age_ms, settings))
        .map(|state| state.pool_id.address)
        .collect()
}

fn latest_pool_state_block(pool_states: &HashMap<Address, PoolState>) -> u64 {
    pool_states
        .values()
        .map(|state| state.block_number)
        .max()
        .unwrap_or(0)
}

fn active_pool_states(runtime: &SearchRuntime) -> Vec<PoolState> {
    runtime
        .active_pool_addresses
        .iter()
        .filter_map(|pool| runtime.pool_states.get(pool).cloned())
        .collect()
}

#[derive(Clone, Copy, Default)]
struct ActivePoolRefresh {
    swept: bool,
    changed: bool,
}

fn refresh_active_pool_cache_if_due(
    runtime: &mut SearchRuntime,
    now: DateTime<Utc>,
    max_pool_state_age_ms: i64,
    settings: &Settings,
) -> ActivePoolRefresh {
    let interval = active_pool_sweep_interval(max_pool_state_age_ms);
    if runtime
        .last_active_pool_sweep
        .is_some_and(|last_sweep| last_sweep.elapsed() < interval)
    {
        return ActivePoolRefresh::default();
    }
    let active_pool_addresses = active_pool_addresses(
        runtime.pool_states.values(),
        now,
        max_pool_state_age_ms,
        settings,
    );
    let changed = active_pool_addresses != runtime.active_pool_addresses;
    runtime.active_pool_addresses = active_pool_addresses;
    runtime.last_active_pool_sweep = Some(Instant::now());
    ActivePoolRefresh {
        swept: true,
        changed,
    }
}

fn force_changed_quote_ready_pools_active(
    runtime: &mut SearchRuntime,
    changed_pools: &HashSet<Address>,
    settings: &Settings,
) -> bool {
    let mut changed = false;
    for pool in changed_pools {
        let Some(state) = runtime.pool_states.get(pool) else {
            continue;
        };
        if is_pool_state_quote_ready(state, settings) && runtime.active_pool_addresses.insert(*pool)
        {
            changed = true;
        }
    }
    changed
}

fn active_pool_sweep_interval(max_pool_state_age_ms: i64) -> Duration {
    let max_age_ms = max_pool_state_age_ms.max(1) as u64;
    let sweep_ms = (max_age_ms / 10).clamp(ACTIVE_POOL_SWEEP_MIN_MS, ACTIVE_POOL_SWEEP_MAX_MS);
    Duration::from_millis(sweep_ms)
}

fn is_pool_state_active(
    state: &PoolState,
    now: DateTime<Utc>,
    max_pool_state_age_ms: i64,
    settings: &Settings,
) -> bool {
    if state.is_stale(now, max_pool_state_age_ms) {
        return false;
    }
    is_pool_state_quote_ready(state, settings)
}

fn is_pool_state_quote_ready(state: &PoolState, settings: &Settings) -> bool {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            if !is_supported_factory(
                state,
                settings.aerodrome_pool_factory,
                &[AERODROME_CLASSIC_FACTORY],
            ) {
                return false;
            }
            is_nonzero_u256(state.reserve0) && is_nonzero_u256(state.reserve1)
        }
        PoolVariant::AerodromeSlipstream => {
            if !is_supported_factory(
                state,
                settings.aerodrome_slipstream_factory,
                &AERODROME_SLIPSTREAM_FACTORIES,
            ) {
                return false;
            }
            state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::UniswapV3 => {
            if !is_supported_factory(state, settings.uniswap_v3_factory, &[UNISWAP_V3_FACTORY]) {
                return false;
            }
            state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::PancakeV3 => {
            if !is_supported_factory(state, settings.pancake_v3_factory, &[PANCAKE_V3_FACTORY]) {
                return false;
            }
            state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::UniswapV4 => {
            if !is_supported_manager(state, settings.uniswap_v4_pool_manager) {
                return false;
            }
            state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::BalancerV3 => {
            is_supported_manager(state, settings.balancer_v3_vault)
                && state.token0 != Address::ZERO
                && state.token1 != Address::ZERO
        }
    }
}

fn is_supported_factory(
    state: &PoolState,
    configured: Option<Address>,
    fallback_supported: &[&str],
) -> bool {
    let Some(factory) = state.factory_address else {
        return false;
    };
    if configured == Some(factory) {
        return true;
    }
    fallback_supported
        .iter()
        .any(|expected| address_eq_str(factory, expected))
}

fn is_supported_manager(state: &PoolState, configured: Option<Address>) -> bool {
    let Some(factory) = state.factory_address else {
        return false;
    };
    configured == Some(factory)
}

fn address_eq_str(address: Address, expected: &str) -> bool {
    expected
        .parse::<Address>()
        .map(|expected| address == expected)
        .unwrap_or(false)
}

fn is_nonzero_u256(value: Option<U256>) -> bool {
    value.is_some_and(|value| !value.is_zero())
}

#[derive(Default)]
struct TickLoadStats {
    cache_hits: u64,
    cache_misses: u64,
    cache_refreshes: u64,
}

async fn load_tick_states_for_pools<P>(
    pool_states: &HashMap<Address, PoolState>,
    tick_states: &mut HashMap<Address, Vec<TickState>>,
    tick_states_loaded_at: &mut HashMap<Address, Instant>,
    pool_store: &P,
    tick_changed_pools: &HashSet<Address>,
    refreshed_tick_pools: &mut HashSet<Address>,
    path_pools: &HashSet<Address>,
) -> Result<TickLoadStats>
where
    P: TickStateStore,
{
    let mut stats = TickLoadStats::default();
    let mut pools_to_refresh = Vec::new();
    for pool in path_pools {
        let Some(state) = pool_states.get(pool) else {
            continue;
        };
        if !matches!(
            state.variant,
            base_arb_common::types::PoolVariant::AerodromeSlipstream
                | base_arb_common::types::PoolVariant::UniswapV3
                | base_arb_common::types::PoolVariant::PancakeV3
                | base_arb_common::types::PoolVariant::UniswapV4
        ) {
            continue;
        }

        let cache_expired = tick_states_loaded_at.get(pool).is_none_or(|loaded_at| {
            loaded_at.elapsed() >= Duration::from_millis(TICK_CACHE_MAX_AGE_MS)
        });
        let should_refresh = !tick_states.contains_key(pool)
            || cache_expired
            || (tick_changed_pools.contains(pool) && !refreshed_tick_pools.contains(pool));
        if should_refresh {
            stats.cache_misses += 1;
            stats.cache_refreshes += 1;
            pools_to_refresh.push(*pool);
        } else {
            stats.cache_hits += 1;
        }
    }

    if !pools_to_refresh.is_empty() {
        let loaded = pool_store.get_pool_ticks_many(&pools_to_refresh).await?;
        let loaded_at = Instant::now();
        for pool in pools_to_refresh {
            tick_states.insert(pool, loaded.get(&pool).cloned().unwrap_or_default());
            tick_states_loaded_at.insert(pool, loaded_at);
            refreshed_tick_pools.insert(pool);
        }
    }

    Ok(stats)
}

async fn publish_candidates<C>(
    candidate_store: &C,
    opportunity_recorder: &OpportunityRecordQueue,
    cycle_stats: &mut SearchCycleStats,
    engine: &strategy::SearchEngine,
    available_pools: &HashSet<Address>,
    max_pool_state_age_ms: i64,
    max_price_impact_bps: u64,
    latest_known_block: u64,
    max_candidate_lag_blocks: u64,
    candidates: Vec<base_arb_common::types::Candidate>,
) -> Result<()>
where
    C: CandidateStore,
{
    let original_candidate_count = candidates.len();
    let candidates = coalesce_candidates_for_publish(candidates, latest_known_block);
    cycle_stats.candidates_coalesced +=
        original_candidate_count.saturating_sub(candidates.len()) as u64;
    let mut valid_candidates = Vec::new();
    for candidate in candidates {
        let candidate_lag = latest_known_block.saturating_sub(candidate.block_number);
        if candidate_lag > max_candidate_lag_blocks {
            debug!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                candidate_block = candidate.block_number,
                latest_known_block,
                candidate_lag,
                max_candidate_lag_blocks,
                "candidate published despite quote block lag; execution hot path owns final timeliness"
            );
        }
        debug!(candidate_id = %candidate.id, "quote generated");
        match risk::validate_candidate(
            &candidate,
            available_pools,
            max_pool_state_age_ms,
            engine.min_expected_profit,
            max_price_impact_bps,
            &engine.whitelist_paths,
        ) {
            Ok(()) => {
                debug!(candidate_id = %candidate.id, "candidate created");
                valid_candidates.push(candidate);
            }
            Err(err) => {
                cycle_stats.record_risk_rejection(&err);
                debug!(candidate_id = %candidate.id, reason = %err, "candidate rejected");
            }
        }
    }
    if !valid_candidates.is_empty() {
        cycle_stats.opportunities_created += valid_candidates.len() as u64;
        candidate_store.push_candidates(&valid_candidates).await?;
        cycle_stats.opportunity_record_queue_dropped +=
            opportunity_recorder.enqueue(valid_candidates) as u64;
    }
    Ok(())
}

async fn quote_and_publish_path_batch<C, T>(
    candidate_store: &C,
    tick_repair_store: &T,
    opportunity_recorder: &OpportunityRecordQueue,
    cycle_stats: &mut SearchCycleStats,
    engine: &strategy::SearchEngine,
    quote_context: &strategy::QuoteContext<'_>,
    available_pools: &HashSet<Address>,
    max_pool_state_age_ms: i64,
    max_price_impact_bps: u64,
    latest_known_block: u64,
    max_candidate_lag_blocks: u64,
    path_batch: &[&strategy::SearchPath],
) -> Result<()>
where
    C: CandidateStore,
    T: TickRepairStore,
{
    let quote_started = Instant::now();
    let (candidates, mut search_stats) = engine
        .search_with_stats_for_paths_with_context(quote_context, path_batch)
        .await?;
    cycle_stats.quote_ms += quote_started.elapsed().as_millis() as u64;
    search_stats.total_paths = 0;
    let tick_repair_pools = search_stats
        .tick_repair_pools
        .iter()
        .copied()
        .collect::<Vec<_>>();
    cycle_stats.tick_repair_queued_pools += tick_repair_pools.len() as u64;
    cycle_stats.search.merge(&search_stats);
    if !tick_repair_pools.is_empty() {
        tick_repair_store
            .mark_tick_repair_pools(tick_repair_pools)
            .await?;
    }
    let publish_started = Instant::now();
    publish_candidates(
        candidate_store,
        opportunity_recorder,
        cycle_stats,
        engine,
        available_pools,
        max_pool_state_age_ms,
        max_price_impact_bps,
        latest_known_block,
        max_candidate_lag_blocks,
        candidates,
    )
    .await?;
    cycle_stats.publish_ms += publish_started.elapsed().as_millis() as u64;
    Ok(())
}

fn missing_tick_pools_for_paths(
    pool_states: &HashMap<Address, PoolState>,
    tick_states: &HashMap<Address, Vec<TickState>>,
    path_pools: &HashSet<Address>,
) -> HashSet<Address> {
    let mut missing = HashSet::new();
    for pool in path_pools {
        let Some(state) = pool_states.get(pool) else {
            continue;
        };
        if requires_initialized_ticks(state)
            && !tick_states.get(pool).is_some_and(|ticks| !ticks.is_empty())
        {
            missing.insert(*pool);
        }
    }
    missing
}

fn requires_initialized_ticks(state: &PoolState) -> bool {
    matches!(
        state.variant,
        PoolVariant::AerodromeSlipstream
            | PoolVariant::UniswapV3
            | PoolVariant::PancakeV3
            | PoolVariant::UniswapV4
    )
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

fn coalesce_candidates_for_publish(
    candidates: Vec<Candidate>,
    latest_known_block: u64,
) -> Vec<Candidate> {
    let mut best_by_key: HashMap<CandidateCoalesceKey, Candidate> = HashMap::new();
    for mut candidate in candidates {
        if latest_known_block != 0 {
            candidate.block_number = latest_known_block;
        }
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
