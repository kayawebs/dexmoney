mod eoa_lane;
mod simulator;
mod tx_manager;

use alloy_primitives::Address;
use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{EoaLaneStatus, TxResult, TxStatus};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, EoaStateStore, FailureStore,
    PendingTransactionStore, RecorderStore,
};
use chrono::Utc;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE: usize = 128;
const MAX_CANDIDATE_DRAIN_PER_CYCLE: usize = 16;
const MAX_SIMULATIONS_PER_CYCLE: usize = 4;
const MIN_CANDIDATE_SEEN_TTL_SECS: u64 = 10;

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
    R: RecorderStore + PendingTransactionStore,
{
    let wallet = configured_wallet(settings)?;
    let operator = operator_address(settings, wallet.as_ref())?;

    if settings.execution_submit_enabled {
        let Some(wallet) = wallet.as_ref() else {
            anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
        };
        let mut lane = eoa_store
            .get_lane_state(wallet.address())
            .await?
            .map(|state| eoa_lane::EoaLane { state })
            .unwrap_or_else(|| eoa_lane::EoaLane::new(wallet.address()));

        reconcile_db_pending_transactions(recorder, provider, wallet.address()).await?;

        if lane.state.status == EoaLaneStatus::Pending {
            handle_pending_lane(&mut lane, eoa_store, recorder, provider, wallet, settings).await?;
            return Ok(());
        }

        sync_idle_lane(&mut lane, provider).await?;
        eoa_store.set_lane_state(lane.state.clone()).await?;
    }

    let candidates = pop_fresh_candidates(candidate_store).await?;
    if candidates.is_empty() {
        debug!("no candidate available");
        return Ok(());
    };
    let current_block = provider.get_block_number().await?;

    let mut simulated = 0usize;
    for candidate in candidates {
        if simulated >= MAX_SIMULATIONS_PER_CYCLE {
            break;
        }
        match handle_candidate(
            candidate_store,
            eoa_store,
            recorder,
            provider,
            settings,
            min_simulated_profit_usdc,
            operator,
            wallet.as_ref(),
            &candidate,
            current_block,
        )
        .await?
        {
            CandidateAction::Submitted => return Ok(()),
            CandidateAction::Simulated => simulated += 1,
            CandidateAction::Skipped => {}
        }
    }

    Ok(())
}

enum CandidateAction {
    Skipped,
    Simulated,
    Submitted,
}

async fn handle_candidate<C, E, R>(
    candidate_store: &C,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    min_simulated_profit_usdc: f64,
    operator: Address,
    wallet: Option<&tx_manager::ExecutionWallet>,
    candidate: &base_arb_common::types::Candidate,
    current_block: u64,
) -> Result<CandidateAction>
where
    C: CandidateStore + FailureStore,
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let block_lag = current_block.saturating_sub(candidate.block_number);
    if block_lag > settings.execution_max_candidate_lag_blocks {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            candidate_block = candidate.block_number,
            current_block,
            block_lag,
            max_lag_blocks = settings.execution_max_candidate_lag_blocks,
            "candidate skipped because it is too far behind current block"
        );
        return Ok(CandidateAction::Skipped);
    }

    let candidate_seen_key = simulator::candidate_block_seen_key(&candidate);
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
        return Ok(CandidateAction::Skipped);
    }
    let route_failure_key = simulator::route_failure_key(&candidate);
    if candidate_store.has_failure_key(&route_failure_key).await? {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            amount_in = %candidate.amount_in,
            min_profit = %candidate.min_profit,
            expected_profit = %candidate.expected_profit,
            "candidate skipped after previous structural route failure for identical path parameters"
        );
        return Ok(CandidateAction::Skipped);
    }
    if candidate_store.has_failure_key(&candidate_seen_key).await? {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            block_number = candidate.block_number,
            amount_in = %candidate.amount_in,
            min_profit = %candidate.min_profit,
            expected_profit = %candidate.expected_profit,
            "candidate skipped after previous simulation for identical path parameters in same block"
        );
        return Ok(CandidateAction::Skipped);
    }
    candidate_store
        .mark_failure_key(&candidate_seen_key, candidate_seen_ttl_secs(settings))
        .await?;

    let simulation = simulator::simulate(
        provider,
        settings,
        operator,
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
        } else if simulator::is_structural_route_failure(&simulation) {
            candidate_store
                .mark_failure_key(&route_failure_key, settings.min_profit_failure_ttl_secs)
                .await?;
            info!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                amount_in = %candidate.amount_in,
                min_profit = %candidate.min_profit,
                expected_profit = %candidate.expected_profit,
                reason = ?simulation.revert_reason,
                ttl_secs = settings.min_profit_failure_ttl_secs,
                "cached structural route failure candidate fingerprint"
            );
        }
        return Ok(CandidateAction::Simulated);
    }

    if candidate.is_expired(Utc::now()) {
        info!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            created_at = %candidate.created_at,
            expires_at = %candidate.expires_at,
            "candidate expired after simulation; skipping tx submission"
        );
        return Ok(CandidateAction::Simulated);
    }

    if !settings.execution_submit_enabled {
        info!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            simulated_profit = %simulation.simulated_profit,
            net_simulated_profit = ?simulation.net_simulated_profit,
            gas_estimate = ?simulation.gas_estimate,
            max_fee_per_gas = ?simulation.max_fee_per_gas,
            max_priority_fee_per_gas = ?simulation.max_priority_fee_per_gas,
            "simulation-only mode; skipping tx submission"
        );
        return Ok(CandidateAction::Simulated);
    }

    let Some(wallet) = wallet.as_ref() else {
        anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
    };
    let mut lane = eoa_store
        .get_lane_state(wallet.address())
        .await?
        .map(|state| eoa_lane::EoaLane { state })
        .unwrap_or_else(|| eoa_lane::EoaLane::new(wallet.address()));
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
                submission.submitted_block,
                submission.gas_limit,
                submission.max_fee_per_gas,
                submission.max_priority_fee_per_gas,
            );
            eoa_store.set_lane_state(lane.state.clone()).await?;
            recorder
                .record_transaction(tx_manager::pending_tx_result(
                    &candidate,
                    wallet.address(),
                    &submission,
                ))
                .await?;
            return Ok(CandidateAction::Submitted);
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

    Ok(CandidateAction::Simulated)
}

fn configured_wallet(settings: &Settings) -> Result<Option<tx_manager::ExecutionWallet>> {
    let Some(private_key) = settings.eoa_private_key_1.as_deref() else {
        return Ok(None);
    };

    let wallet = tx_manager::ExecutionWallet::from_private_key(private_key, settings.chain_id)?;
    if let Some(configured_address) = settings.eoa_address_1 {
        if configured_address != wallet.address() {
            warn!(
                configured_address = %configured_address,
                wallet_address = %wallet.address(),
                "EOA_ADDRESS_1 does not match EOA_PRIVATE_KEY_1; using private-key-derived address"
            );
        }
    }
    Ok(Some(wallet))
}

fn operator_address(
    settings: &Settings,
    wallet: Option<&tx_manager::ExecutionWallet>,
) -> Result<Address> {
    if let Some(wallet) = wallet {
        return Ok(wallet.address());
    }
    if let Some(address) = settings.eoa_address_1 {
        return Ok(address);
    }
    anyhow::bail!("EOA_ADDRESS_1 or EOA_PRIVATE_KEY_1 is required for simulation");
}

fn candidate_seen_ttl_secs(settings: &Settings) -> u64 {
    let ttl_from_candidate = settings
        .candidate_ttl_ms
        .max(0)
        .saturating_add(999)
        .checked_div(1000)
        .unwrap_or(0) as u64;
    ttl_from_candidate.saturating_add(MIN_CANDIDATE_SEEN_TTL_SECS)
}

async fn pop_fresh_candidates<C>(
    candidate_store: &C,
) -> Result<Vec<base_arb_common::types::Candidate>>
where
    C: CandidateStore,
{
    let now = Utc::now();
    let mut expired = 0usize;
    let mut candidates = Vec::new();

    for _ in 0..MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE {
        let Some(candidate) = candidate_store.pop_candidate().await? else {
            break;
        };
        if candidate.is_expired(now) {
            expired += 1;
            continue;
        }
        candidates.push(candidate);
        if candidates.len() >= MAX_CANDIDATE_DRAIN_PER_CYCLE {
            break;
        }
    }

    if expired > 0 {
        info!(expired, "drained expired candidates from queue");
    }
    candidates.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));
    Ok(candidates)
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
    wallet: &tx_manager::ExecutionWallet,
    settings: &Settings,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
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
        if clear_lane_if_pending_nonce_consumed(
            lane,
            eoa_store,
            recorder,
            provider,
            tx_hash,
            opportunity_id,
            simulation_id,
            nonce,
        )
        .await?
        {
            return Ok(());
        }
        maybe_replace_pending_tx(lane, eoa_store, recorder, provider, wallet, settings).await?;
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

async fn clear_lane_if_pending_nonce_consumed<E, R>(
    lane: &mut eoa_lane::EoaLane,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    tx_hash: alloy_primitives::B256,
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    nonce: u64,
) -> Result<bool>
where
    E: EoaStateStore,
    R: RecorderStore,
{
    let confirmed_nonce = provider
        .get_transaction_count(lane.state.address, false)
        .await?;
    if confirmed_nonce <= nonce {
        return Ok(false);
    }

    let pending_nonce = provider
        .get_transaction_count(lane.state.address, true)
        .await?;
    let eth_balance = provider.get_balance(lane.state.address).await?;
    recorder
        .record_transaction(tx_manager::dropped_consumed_nonce_tx_result(
            opportunity_id,
            simulation_id,
            lane.state.address,
            tx_hash,
            nonce,
            confirmed_nonce,
        ))
        .await?;
    lane.clear_consumed_pending(confirmed_nonce, pending_nonce, eth_balance);
    eoa_store.set_lane_state(lane.state.clone()).await?;
    warn!(
        tx_hash = %tx_hash,
        nonce,
        confirmed_nonce,
        pending_nonce,
        "pending tx has no receipt but its nonce is already consumed; cleared EOA lane"
    );
    Ok(true)
}

async fn maybe_replace_pending_tx<E, R>(
    lane: &mut eoa_lane::EoaLane,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    wallet: &tx_manager::ExecutionWallet,
    settings: &Settings,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    if lane.state.pending_replacement_count >= settings.execution_max_replacements {
        return Ok(());
    }
    let Some(submitted_block) = lane.state.pending_submitted_block else {
        return Ok(());
    };
    let current_block = provider.get_block_number().await?;
    let pending_blocks = current_block.saturating_sub(submitted_block);
    if pending_blocks < settings.execution_pending_replacement_blocks {
        return Ok(());
    }

    let Some(opportunity_id) = lane.state.pending_opportunity_id else {
        return Ok(());
    };
    let Some(simulation_id) = lane.state.pending_simulation_id else {
        return Ok(());
    };
    let Some(nonce) = lane.state.pending_nonce else {
        return Ok(());
    };
    let Some(gas_limit) = lane.state.pending_gas_limit else {
        return Ok(());
    };
    let Some(previous_max_fee_per_gas) = lane.state.pending_max_fee_per_gas else {
        return Ok(());
    };
    let Some(previous_max_priority_fee_per_gas) = lane.state.pending_max_priority_fee_per_gas
    else {
        return Ok(());
    };
    let Some(old_tx_hash) = lane.state.pending_tx else {
        return Ok(());
    };
    let Some(calldata) = recorder.simulation_calldata(simulation_id).await? else {
        warn!(
            simulation_id = %simulation_id,
            "cannot replace pending tx because simulation calldata is missing"
        );
        return Ok(());
    };

    match tx_manager::replace_pending_transaction(
        provider,
        wallet,
        settings,
        &calldata,
        nonce,
        gas_limit,
        previous_max_fee_per_gas,
        previous_max_priority_fee_per_gas,
    )
    .await
    {
        Ok(submission) => {
            recorder
                .record_transaction(tx_manager::dropped_replaced_tx_result(
                    opportunity_id,
                    Some(simulation_id),
                    lane.state.address,
                    old_tx_hash,
                    nonce,
                    submission.tx_hash,
                ))
                .await?;
            recorder
                .record_transaction(tx_manager::pending_replacement_tx_result(
                    opportunity_id,
                    Some(simulation_id),
                    lane.state.address,
                    &submission,
                ))
                .await?;
            lane.mark_replaced(
                submission.tx_hash,
                submission.submitted_block,
                submission.gas_limit,
                submission.max_fee_per_gas,
                submission.max_priority_fee_per_gas,
            );
            eoa_store.set_lane_state(lane.state.clone()).await?;
            info!(
                opportunity_id = %opportunity_id,
                old_tx_hash = %old_tx_hash,
                new_tx_hash = %submission.tx_hash,
                nonce,
                pending_blocks,
                replacement_count = lane.state.pending_replacement_count,
                "pending tx replaced with higher fee"
            );
        }
        Err(err) => {
            warn!(
                opportunity_id = %opportunity_id,
                tx_hash = %old_tx_hash,
                nonce,
                pending_blocks,
                error = %err,
                "pending tx replacement failed"
            );
        }
    }

    Ok(())
}

async fn reconcile_db_pending_transactions<R>(
    recorder: &R,
    provider: &ChainProvider,
    eoa: alloy_primitives::Address,
) -> Result<()>
where
    R: RecorderStore + PendingTransactionStore,
{
    let confirmed_nonce = provider.get_transaction_count(eoa, false).await?;
    for pending in recorder.pending_transactions_for_eoa(eoa, 20).await? {
        let Some(receipt) = provider.get_transaction_receipt(pending.tx_hash).await? else {
            if confirmed_nonce > pending.nonce {
                recorder
                    .record_transaction(tx_manager::dropped_consumed_nonce_tx_result(
                        pending.opportunity_id,
                        pending.simulation_id,
                        pending.eoa,
                        pending.tx_hash,
                        pending.nonce,
                        confirmed_nonce,
                    ))
                    .await?;
                warn!(
                    tx_hash = %pending.tx_hash,
                    nonce = pending.nonce,
                    confirmed_nonce,
                    "reconciled pending transaction as dropped because its nonce is already consumed"
                );
            }
            continue;
        };
        recorder
            .record_transaction(tx_manager::receipt_tx_result(
                pending.opportunity_id,
                pending.simulation_id,
                pending.eoa,
                pending.nonce,
                &receipt,
            ))
            .await?;
        debug!(
            tx_hash = %pending.tx_hash,
            block_number = ?receipt.block_number,
            success = receipt.success,
            "reconciled pending transaction from receipt"
        );
    }
    Ok(())
}
