mod opportunity;
mod risk;
mod strategy;

use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, PoolStateStore, RecorderStore,
};
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    info!("searcher initialized");
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        run_search_cycle(
            &redis,
            &redis,
            &postgres,
            &settings,
            settings.candidate_ttl_ms,
            settings.max_price_impact_bps,
            settings.min_expected_profit_usdc,
        )
        .await?;
    }
}

async fn run_search_cycle<P, C, R>(
    pool_store: &P,
    candidate_store: &C,
    recorder: &R,
    settings: &Settings,
    candidate_ttl_ms: i64,
    max_price_impact_bps: u64,
    min_expected_profit_usdc: f64,
) -> Result<()>
where
    P: PoolStateStore,
    C: CandidateStore,
    R: RecorderStore,
{
    let engine = strategy::engine_from_settings(
        settings,
        candidate_ttl_ms,
        max_price_impact_bps,
        strategy::usdc_to_units(min_expected_profit_usdc),
    )?;
    let pool_states = pool_store.all_pool_states().await?;
    if pool_states.is_empty() {
        info!("no pool states available in redis");
        return Ok(());
    }
    let candidates = engine.search(&pool_states)?;

    for candidate in candidates {
        info!(candidate_id = %candidate.id, "quote generated");
        match risk::validate_candidate(
            &candidate,
            &pool_states,
            candidate_ttl_ms,
            engine.min_expected_profit,
            max_price_impact_bps,
            &engine.whitelist_paths,
        ) {
            Ok(()) => {
                recorder.record_opportunity(candidate.clone()).await?;
                candidate_store.push_candidate(candidate.clone()).await?;
                info!(candidate_id = %candidate.id, "candidate created");
            }
            Err(err) => info!(candidate_id = %candidate.id, reason = %err, "candidate rejected"),
        }
    }

    Ok(())
}
