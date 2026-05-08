mod eoa_lane;
mod simulator;
mod tx_manager;

use alloy_primitives::address;
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, EoaStateStore, RecorderStore,
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

    info!("execution-manager initialized");
    let mut ticker = interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        run_execution_cycle(
            &redis,
            &redis,
            &postgres,
            settings.min_simulated_profit_usdc,
        )
        .await?;
    }
}

async fn run_execution_cycle<C, E, R>(
    candidate_store: &C,
    eoa_store: &E,
    recorder: &R,
    min_simulated_profit_usdc: f64,
) -> Result<()>
where
    C: CandidateStore,
    E: EoaStateStore,
    R: RecorderStore,
{
    let lane_address = address!("4200000000000000000000000000000000000006");
    let mut lane = eoa_lane::EoaLane::new(lane_address);
    eoa_store.set_lane_state(lane.state.clone()).await?;
    info!(lane = ?lane.state, "eoa lane ready");

    let Some(candidate) = candidate_store.pop_candidate().await? else {
        info!("no candidate available");
        return Ok(());
    };

    let simulation = simulator::simulate(&candidate, min_simulated_profit_usdc);
    recorder.record_simulation(simulation.clone()).await?;
    info!(candidate_id = %candidate.id, success = simulation.success, "simulation success/fail");

    if simulation.success {
        let pending = tx_manager::build_pending_tx(&candidate, lane.state.local_nonce);
        let tx_hash = tx_manager::synthetic_tx_hash(&candidate, lane.state.local_nonce);
        lane.mark_submitted(tx_hash);
        eoa_store.set_lane_state(lane.state.clone()).await?;
        recorder.record_transaction(pending.clone()).await?;
        info!(candidate_id = %candidate.id, nonce = pending.nonce, "tx submitted");

        lane.mark_confirmed(lane.state.local_nonce);
        eoa_store.set_lane_state(lane.state.clone()).await?;
        info!(candidate_id = %candidate.id, "tx confirmed/reverted");
    }

    Ok(())
}
