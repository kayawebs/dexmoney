mod eoa_lane;
mod simulator;

use alloy_primitives::address;
use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, EoaStateStore, RecorderStore,
};
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);

    info!("execution-manager initialized");
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        run_execution_cycle(
            &redis,
            &redis,
            &postgres,
            &provider,
            &settings,
            settings.min_simulated_profit_usdc,
        )
        .await?;
    }
}

async fn run_execution_cycle<C, E, R>(
    candidate_store: &C,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    min_simulated_profit_usdc: f64,
) -> Result<()>
where
    C: CandidateStore,
    E: EoaStateStore,
    R: RecorderStore,
{
    let lane_address = settings
        .eoa_address_1
        .unwrap_or_else(|| address!("0000000000000000000000000000000000000000"));
    let lane = eoa_lane::EoaLane::new(lane_address);
    eoa_store.set_lane_state(lane.state.clone()).await?;
    info!(lane = ?lane.state, "eoa lane ready");

    let Some(candidate) = candidate_store.pop_candidate().await? else {
        info!("no candidate available");
        return Ok(());
    };

    let simulation =
        simulator::simulate(provider, settings, &candidate, min_simulated_profit_usdc).await;
    recorder.record_simulation(simulation.clone()).await?;
    info!(candidate_id = %candidate.id, success = simulation.success, "simulation success/fail");

    if simulation.success {
        info!(
            candidate_id = %candidate.id,
            "eth_call simulation passed; raw transaction signing/submission is not enabled yet"
        );
    }

    Ok(())
}
