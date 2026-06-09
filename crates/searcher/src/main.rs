mod opportunity;
mod risk;
mod strategy;

use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::errors::ArbBotError;
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, PairSearchConfigStore,
    PoolStateStore, RecorderStore, TickStateStore,
};
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

    loop {
        ticker.tick().await;
        let stats = run_search_cycle(
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

async fn run_search_cycle<P, C, R>(
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

    Ok(cycle_stats)
}
