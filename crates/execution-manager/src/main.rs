mod eoa_lane;
mod simulator;
mod tx_manager;

use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{EoaLaneStatus, TxResult, TxStatus};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, EoaStateStore, FailureStore,
    RecorderStore,
};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE: usize = 128;

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
    C: CandidateStore + FailureStore,
    E: EoaStateStore,
    R: RecorderStore,
{
    let private_key = settings
        .eoa_private_key_1
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("EOA_PRIVATE_KEY_1 is required"))?;
    let wallet = tx_manager::ExecutionWallet::from_private_key(private_key, settings.chain_id)?;
    if let Some(configured) = settings.eoa_address_1 {
        if configured != wallet.address() {
            warn!(
                configured = %configured,
                derived = %wallet.address(),
                "EOA_ADDRESS_1 does not match EOA_PRIVATE_KEY_1 derived address"
            );
        }
    }

    let mut lane = eoa_store
        .get_lane_state(wallet.address())
        .await?
        .map(|state| eoa_lane::EoaLane { state })
        .unwrap_or_else(|| eoa_lane::EoaLane::new(wallet.address()));

    if lane.state.status == EoaLaneStatus::Pending {
        handle_pending_lane(&mut lane, eoa_store, recorder, provider).await?;
        return Ok(());
    }

    sync_idle_lane(&mut lane, provider).await?;
    eoa_store.set_lane_state(lane.state.clone()).await?;

    let Some(candidate) = pop_fresh_candidate(candidate_store).await? else {
        debug!("no candidate available");
        return Ok(());
    };
    let min_profit_failure_key = simulator::min_profit_failure_key(&candidate);
    if candidate_store
        .has_failure_key(&min_profit_failure_key)
        .await?
    {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            amount_in = %candidate.amount_in,
            min_profit = %candidate.min_profit,
            expected_profit = %candidate.expected_profit,
            "candidate skipped after previous MinProfitNotMet for identical path parameters"
        );
        return Ok(());
    }

    let simulation = simulator::simulate(
        provider,
        settings,
        wallet.address(),
        &candidate,
        min_simulated_profit_usdc,
    )
    .await;
    recorder.record_simulation(simulation.clone()).await?;
    debug!(candidate_id = %candidate.id, success = simulation.success, "simulation success/fail");

    if !simulation.success {
        if simulator::is_min_profit_not_met(&simulation) {
            candidate_store
                .mark_failure_key(
                    &min_profit_failure_key,
                    settings.min_profit_failure_ttl_secs,
                )
                .await?;
            info!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                amount_in = %candidate.amount_in,
                min_profit = %candidate.min_profit,
                expected_profit = %candidate.expected_profit,
                ttl_secs = settings.min_profit_failure_ttl_secs,
                "cached MinProfitNotMet candidate fingerprint"
            );
        }
        return Ok(());
    }

    let nonce = lane.state.local_nonce;
    match tx_manager::submit_candidate(provider, &wallet, settings, &candidate, &simulation, nonce)
        .await
    {
        Ok(submission) => {
            lane.mark_submitted(
                candidate.id,
                submission.simulation_id,
                submission.tx_hash,
                submission.nonce,
            );
            eoa_store.set_lane_state(lane.state.clone()).await?;
            recorder
                .record_transaction(tx_manager::pending_tx_result(
                    &candidate,
                    wallet.address(),
                    &submission,
                ))
                .await?;
        }
        Err(err) => {
            warn!(
                candidate_id = %candidate.id,
                nonce,
                error = %err,
                "tx submission failed"
            );
            lane.mark_cooldown();
            eoa_store.set_lane_state(lane.state.clone()).await?;
            recorder
                .record_transaction(TxResult {
                    opportunity_id: candidate.id,
                    simulation_id: Some(simulation.id),
                    eoa: wallet.address(),
                    tx_hash: None,
                    nonce,
                    status: TxStatus::Dropped,
                    realized_profit: None,
                    gas_used: None,
                    effective_gas_price: None,
                    revert_reason: Some(err.to_string()),
                    receipt_json: None,
                })
                .await?;
        }
    }

    Ok(())
}

async fn pop_fresh_candidate<C>(
    candidate_store: &C,
) -> Result<Option<base_arb_common::types::Candidate>>
where
    C: CandidateStore,
{
    let now = Utc::now();
    let mut expired = 0usize;

    for _ in 0..MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE {
        let Some(candidate) = candidate_store.pop_candidate().await? else {
            if expired > 0 {
                info!(expired, "drained expired candidates from queue");
            }
            return Ok(None);
        };
        if candidate.is_expired(now) {
            expired += 1;
            continue;
        }
        if expired > 0 {
            info!(expired, "drained expired candidates from queue");
        }
        return Ok(Some(candidate));
    }

    info!(
        expired,
        "expired candidate drain limit reached; continuing next cycle"
    );
    Ok(None)
}

async fn sync_idle_lane(lane: &mut eoa_lane::EoaLane, provider: &ChainProvider) -> Result<()> {
    let confirmed_nonce = provider
        .get_transaction_count(lane.state.address, false)
        .await?;
    let pending_nonce = provider
        .get_transaction_count(lane.state.address, true)
        .await?;
    lane.state.confirmed_nonce = confirmed_nonce;
    lane.state.local_nonce = pending_nonce;
    lane.state.eth_balance = provider.get_balance(lane.state.address).await?;
    lane.state.status = EoaLaneStatus::Idle;
    Ok(())
}

async fn handle_pending_lane<E, R>(
    lane: &mut eoa_lane::EoaLane,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore,
{
    let Some(tx_hash) = lane.state.pending_tx else {
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(());
    };
    let Some(opportunity_id) = lane.state.pending_opportunity_id else {
        warn!(tx_hash = %tx_hash, "pending lane is missing opportunity id");
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(());
    };
    let Some(nonce) = lane.state.pending_nonce else {
        warn!(tx_hash = %tx_hash, "pending lane is missing nonce");
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(());
    };
    let simulation_id = lane.state.pending_simulation_id;

    let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? else {
        debug!(tx_hash = %tx_hash, "pending tx has no receipt yet");
        return Ok(());
    };

    recorder
        .record_transaction(tx_manager::receipt_tx_result(
            opportunity_id,
            simulation_id,
            lane.state.address,
            nonce,
            &receipt,
        ))
        .await?;
    let confirmed_nonce = provider
        .get_transaction_count(lane.state.address, false)
        .await?;
    lane.mark_confirmed(confirmed_nonce);
    lane.state.local_nonce = provider
        .get_transaction_count(lane.state.address, true)
        .await?;
    lane.state.eth_balance = provider.get_balance(lane.state.address).await?;
    eoa_store.set_lane_state(lane.state.clone()).await?;

    if receipt.success {
        info!(
            candidate_id = %opportunity_id,
            tx_hash = %tx_hash,
            block_number = ?receipt.block_number,
            "tx confirmed"
        );
    } else {
        warn!(
            tx_hash = %tx_hash,
            block_number = ?receipt.block_number,
            "tx reverted"
        );
    }

    Ok(())
}
