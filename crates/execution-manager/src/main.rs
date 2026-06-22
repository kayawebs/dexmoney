mod eoa_lane;
mod simulator;
mod tx_manager;

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{Candidate, EoaLaneStatus, SimulationResult, TxResult, TxStatus};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, CurrentBlockStore, EoaStateStore,
    PendingTransactionStore, RecorderStore,
};
use chrono::Utc;
use std::collections::{BTreeMap, HashSet};
use tokio::task::JoinSet;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE: usize = 2_048;
const MAX_CANDIDATE_DRAIN_PER_CYCLE: usize = 128;
const MAX_SIMULATIONS_PER_CYCLE: usize = MAX_CANDIDATE_DRAIN_PER_CYCLE;

#[derive(Debug, Clone)]
struct ExecutionRuntime {
    fund_wallet: Option<tx_manager::ExecutionWallet>,
    worker_wallets: Vec<tx_manager::ExecutionWallet>,
    simulation_operator: Option<Address>,
    worker_min_balance: U256,
    worker_target_balance: U256,
    executor_contracts: Vec<Address>,
}

impl ExecutionRuntime {
    fn from_settings(settings: &Settings) -> Result<Self> {
        let fund_wallet = configured_fund_wallet(settings)?;
        let worker_wallets = configured_worker_wallets(settings, fund_wallet.as_ref())?;
        let simulation_operator = fund_wallet
            .as_ref()
            .map(tx_manager::ExecutionWallet::address)
            .or(settings.eoa_address_1);
        let worker_min_balance = parse_config_u256(
            settings.execution_worker_min_balance_wei.as_deref(),
            "execution_worker_min_balance_wei",
        )?;
        let worker_target_balance = parse_config_u256(
            settings.execution_worker_target_balance_wei.as_deref(),
            "execution_worker_target_balance_wei",
        )?
        .max(worker_min_balance);
        Ok(Self {
            fund_wallet,
            worker_wallets,
            simulation_operator,
            worker_min_balance,
            worker_target_balance,
            executor_contracts: configured_executor_contracts(settings),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);
    let runtime = ExecutionRuntime::from_settings(&settings)?;
    let cleared_candidates = redis.clear_candidates().await?;
    if cleared_candidates > 0 {
        info!(
            cleared_candidates,
            "cleared stale candidate queue on execution-manager startup"
        );
    }

    if settings.execution_submit_enabled {
        let workers = runtime
            .worker_wallets
            .iter()
            .map(|wallet| wallet.address().to_string())
            .collect::<Vec<_>>()
            .join(",");
        info!(
            fund = ?runtime.fund_wallet.as_ref().map(tx_manager::ExecutionWallet::address),
            workers,
            worker_count = runtime.worker_wallets.len(),
            "execution EOA pool configured"
        );
    }

    info!("execution-manager initialized");
    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut circuit_breaker = ExecutionCircuitBreaker::new(settings.execution_failure_rate_min_txs);

    loop {
        ticker.tick().await;
        run_execution_cycle(
            &redis,
            &redis,
            &postgres,
            &provider,
            &settings,
            &runtime,
            settings.min_simulated_profit_usdc,
            &mut circuit_breaker,
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
    runtime: &ExecutionRuntime,
    min_simulated_profit_usdc: f64,
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<()>
where
    C: CandidateStore + CurrentBlockStore,
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let max_candidate_lag_blocks = settings.execution_max_candidate_lag_blocks.max(1);
    let Some(current_block) = candidate_store.get_current_block().await? else {
        debug!("current block not available from market-data");
        return Ok(());
    };
    let candidates =
        pop_fresh_candidates(candidate_store, current_block, max_candidate_lag_blocks).await?;
    if candidates.is_empty() {
        if settings.execution_submit_enabled {
            let Some(fund_wallet) = runtime.fund_wallet.as_ref() else {
                anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
            };
            prepare_eoa_pool(
                eoa_store,
                recorder,
                provider,
                settings,
                runtime,
                fund_wallet,
                circuit_breaker,
            )
            .await?;
        }
        debug!("no candidate available");
        return Ok(());
    };

    let selected_wallet = if settings.execution_submit_enabled {
        let Some(fund_wallet) = runtime.fund_wallet.as_ref() else {
            anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
        };
        if circuit_breaker.is_halted(settings) {
            return Ok(());
        }
        let cached = select_cached_ready_worker(eoa_store, runtime).await?;
        match cached {
            Some(wallet) => Some(wallet),
            None => {
                let prepared = prepare_eoa_pool(
                    eoa_store,
                    recorder,
                    provider,
                    settings,
                    runtime,
                    fund_wallet,
                    circuit_breaker,
                )
                .await?;
                if prepared.is_none() || circuit_breaker.is_halted(settings) {
                    return Ok(());
                }
                prepared
            }
        }
    } else {
        None
    };
    let operator = selected_wallet
        .as_ref()
        .map(tx_manager::ExecutionWallet::address)
        .or(runtime.simulation_operator)
        .context("EOA_ADDRESS_1 or EOA_PRIVATE_KEY_1 is required for simulation")?;
    if let Some(wallet) = selected_wallet.as_ref() {
        info!(
            worker = %wallet.address(),
            candidate_count = candidates.len(),
            "execution worker selected for candidate batch"
        );
    }
    let batch_started = Instant::now();
    let simulation_context = simulator::SimulationContext::load(provider, settings).await;
    if !settings.execution_submit_enabled {
        return run_dry_run_simulation_batch(
            recorder,
            provider,
            settings,
            operator,
            candidates,
            min_simulated_profit_usdc,
            simulation_context,
            current_block,
            max_candidate_lag_blocks,
            batch_started,
        )
        .await;
    }
    run_live_submission_batch(
        eoa_store,
        recorder,
        provider,
        settings,
        operator,
        selected_wallet.as_ref(),
        runtime.fund_wallet.as_ref(),
        candidates,
        min_simulated_profit_usdc,
        simulation_context,
        current_block,
        max_candidate_lag_blocks,
        batch_started,
        circuit_breaker,
    )
    .await
}

async fn run_dry_run_simulation_batch<R>(
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    operator: Address,
    candidates: Vec<Candidate>,
    min_simulated_profit_usdc: f64,
    simulation_context: simulator::SimulationContext,
    current_block: u64,
    max_candidate_lag_blocks: u64,
    batch_started: Instant,
) -> Result<()>
where
    R: RecorderStore,
{
    let candidate_count = candidates.len();
    let max_to_simulate = candidate_count.min(MAX_SIMULATIONS_PER_CYCLE);
    let concurrency = settings
        .execution_simulation_concurrency
        .max(1)
        .min(MAX_SIMULATIONS_PER_CYCLE as u64) as usize;
    let mut candidates = candidates.into_iter().take(max_to_simulate);
    let mut join_set = JoinSet::<SimulationResult>::new();
    let mut spawned = 0usize;
    let mut simulated = 0usize;
    let mut simulation_successes = 0usize;
    let mut simulation_failures = 0usize;
    let mut join_errors = 0usize;
    let mut simulation_records = Vec::with_capacity(max_to_simulate);

    loop {
        while spawned < max_to_simulate && join_set.len() < concurrency {
            let Some(candidate) = candidates.next() else {
                break;
            };
            spawned += 1;
            let provider = provider.clone();
            let settings = settings.clone();
            let simulation_context = simulation_context.clone();
            join_set.spawn(async move {
                simulator::simulate(
                    &provider,
                    &settings,
                    operator,
                    &candidate,
                    min_simulated_profit_usdc,
                    &simulation_context,
                )
                .await
            });
        }

        if join_set.is_empty() {
            break;
        }

        match join_set.join_next().await {
            Some(Ok(simulation)) => {
                debug!(
                    candidate_id = %simulation.opportunity_id,
                    success = simulation.success,
                    "simulation success/fail"
                );
                if simulation.success {
                    simulation_successes += 1;
                    info!(
                        candidate_id = %simulation.opportunity_id,
                        path = ?simulation.path_name,
                        simulated_profit = %simulation.simulated_profit,
                        net_simulated_profit = ?simulation.net_simulated_profit,
                        gas_estimate = ?simulation.gas_estimate,
                        max_fee_per_gas = ?simulation.max_fee_per_gas,
                        max_priority_fee_per_gas = ?simulation.max_priority_fee_per_gas,
                        "simulation-only mode; skipping tx submission"
                    );
                } else {
                    simulation_failures += 1;
                }
                simulation_records.push(simulation);
                simulated += 1;
            }
            Some(Err(err)) => {
                join_errors += 1;
                warn!(error = %err, "dry-run simulation task failed");
            }
            None => break,
        }
    }

    recorder.record_simulations(simulation_records).await?;

    info!(
        candidate_count,
        current_block,
        max_candidate_lag_blocks,
        simulated,
        simulation_successes,
        simulation_failures,
        join_errors,
        concurrency,
        skipped = candidate_count.saturating_sub(simulated),
        elapsed_ms = batch_started.elapsed().as_millis() as u64,
        skipped_by_reason = ?BTreeMap::<&'static str, usize>::new(),
        "execution candidate batch summary"
    );

    Ok(())
}

#[derive(Debug)]
struct LiveSimulationOutcome {
    candidate: Candidate,
    simulation: SimulationResult,
}

async fn run_live_submission_batch<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    operator: Address,
    wallet: Option<&tx_manager::ExecutionWallet>,
    fund_wallet: Option<&tx_manager::ExecutionWallet>,
    candidates: Vec<Candidate>,
    min_simulated_profit_usdc: f64,
    simulation_context: simulator::SimulationContext,
    current_block: u64,
    max_candidate_lag_blocks: u64,
    batch_started: Instant,
    circuit_breaker: &ExecutionCircuitBreaker,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let candidate_count = candidates.len();
    let mut skipped_by_reason: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut approval_ready = Vec::new();
    let mut approved_cache = HashSet::new();
    let mut preflight_simulated = 0usize;

    for candidate in candidates {
        if approval_ready.len() >= MAX_SIMULATIONS_PER_CYCLE {
            break;
        }
        let block_lag = current_block.saturating_sub(candidate.block_number);
        if block_lag > max_candidate_lag_blocks {
            *skipped_by_reason.entry("block_lag").or_insert(0) += 1;
            warn!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                candidate_block = candidate.block_number,
                current_block,
                block_lag,
                max_lag_blocks = max_candidate_lag_blocks,
                "fresh candidate unexpectedly skipped because it is too far behind current block"
            );
            continue;
        }

        if let Some(action) = live_approval_preflight(
            eoa_store,
            recorder,
            provider,
            settings,
            wallet,
            fund_wallet,
            &candidate,
            current_block,
            &mut approved_cache,
        )
        .await?
        {
            match action {
                CandidateAction::Submitted => {
                    info!(
                        candidate_count,
                        current_block,
                        max_candidate_lag_blocks,
                        simulated = preflight_simulated,
                        skipped = skipped_by_reason.values().sum::<usize>(),
                        elapsed_ms = batch_started.elapsed().as_millis() as u64,
                        skipped_by_reason = ?skipped_by_reason,
                        "execution candidate batch summary"
                    );
                    return Ok(());
                }
                CandidateAction::Simulated => preflight_simulated += 1,
                CandidateAction::Skipped(reason) => {
                    *skipped_by_reason.entry(reason).or_insert(0) += 1;
                }
            }
            continue;
        }

        approval_ready.push(candidate);
    }

    let max_to_simulate = approval_ready.len().min(MAX_SIMULATIONS_PER_CYCLE);
    let concurrency = settings
        .execution_simulation_concurrency
        .max(1)
        .min(MAX_SIMULATIONS_PER_CYCLE as u64) as usize;
    let mut candidates = approval_ready.into_iter().take(max_to_simulate);
    let mut join_set = JoinSet::<LiveSimulationOutcome>::new();
    let mut spawned = 0usize;
    let mut simulated = preflight_simulated;
    let mut simulation_successes = 0usize;
    let mut simulation_failures = 0usize;
    let mut join_errors = 0usize;
    let mut successful = Vec::new();
    let mut simulation_records = Vec::with_capacity(max_to_simulate);

    loop {
        while spawned < max_to_simulate && join_set.len() < concurrency {
            let Some(candidate) = candidates.next() else {
                break;
            };
            spawned += 1;
            let provider = provider.clone();
            let settings = settings.clone();
            let simulation_context = simulation_context.clone();
            join_set.spawn(async move {
                let simulation = simulator::simulate(
                    &provider,
                    &settings,
                    operator,
                    &candidate,
                    min_simulated_profit_usdc,
                    &simulation_context,
                )
                .await;
                LiveSimulationOutcome {
                    candidate,
                    simulation,
                }
            });
        }

        if join_set.is_empty() {
            break;
        }

        match join_set.join_next().await {
            Some(Ok(outcome)) => {
                debug!(
                    candidate_id = %outcome.simulation.opportunity_id,
                    success = outcome.simulation.success,
                    "simulation success/fail"
                );
                simulation_records.push(outcome.simulation.clone());
                if outcome.simulation.success {
                    simulation_successes += 1;
                    successful.push(outcome);
                } else {
                    simulation_failures += 1;
                }
                simulated += 1;
            }
            Some(Err(err)) => {
                join_errors += 1;
                warn!(error = %err, "live simulation task failed");
            }
            None => break,
        }
    }

    recorder.record_simulations(simulation_records).await?;

    successful.sort_by(|left, right| {
        simulation_profit_rank(&right.simulation).cmp(&simulation_profit_rank(&left.simulation))
    });

    let mut submitted = false;
    for outcome in successful {
        match submit_simulated_candidate(
            eoa_store,
            recorder,
            provider,
            settings,
            wallet,
            circuit_breaker,
            &outcome.candidate,
            &outcome.simulation,
        )
        .await?
        {
            CandidateAction::Submitted => {
                submitted = true;
                break;
            }
            CandidateAction::Simulated => {}
            CandidateAction::Skipped(reason) => {
                *skipped_by_reason.entry(reason).or_insert(0) += 1;
            }
        }
    }

    info!(
        candidate_count,
        current_block,
        max_candidate_lag_blocks,
        simulated,
        simulation_successes,
        simulation_failures,
        join_errors,
        concurrency,
        submitted,
        skipped = skipped_by_reason.values().sum::<usize>(),
        elapsed_ms = batch_started.elapsed().as_millis() as u64,
        skipped_by_reason = ?skipped_by_reason,
        "execution candidate batch summary"
    );

    Ok(())
}

fn simulation_profit_rank(simulation: &SimulationResult) -> U256 {
    simulation
        .net_simulated_profit
        .unwrap_or(simulation.simulated_profit)
}

async fn live_approval_preflight<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    wallet: Option<&tx_manager::ExecutionWallet>,
    fund_wallet: Option<&tx_manager::ExecutionWallet>,
    candidate: &Candidate,
    current_block: u64,
    approved_cache: &mut HashSet<(Address, Address, Address)>,
) -> Result<Option<CandidateAction>>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    if !settings.execution_submit_enabled {
        return Ok(None);
    }
    if wallet.is_none() {
        anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
    }

    let approvals = simulator::required_token_approvals(candidate, settings)?;
    let missing_approvals = tx_manager::missing_approvals_cached(
        provider,
        settings,
        candidate,
        &approvals,
        approved_cache,
    )
    .await
    .unwrap_or_else(|err| {
        warn!(
            candidate_id = %candidate.id,
            error = %err,
            "failed to check route allowances; trying route approvals before tx"
        );
        approvals
    });
    if missing_approvals.is_empty() {
        return Ok(None);
    }

    let Some(worker_wallet) = wallet else {
        anyhow::bail!("execution worker EOA is required for lazy approval follow-up tx");
    };
    let Some(fund_wallet) = fund_wallet else {
        anyhow::bail!("EOA_PRIVATE_KEY_1 is required for executor approval admin txs");
    };
    if admin_has_pending_nonce(provider, fund_wallet.address()).await? {
        debug!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            "candidate skipped because fund/admin EOA has a pending tx"
        );
        return Ok(Some(CandidateAction::Skipped("admin_pending_approval")));
    }

    let executor = simulator::executor_for_candidate(settings, candidate)?;
    let nonce = provider
        .get_transaction_count(fund_wallet.address(), true)
        .await?;
    let approval = missing_approvals[0];
    let calldata = simulator::build_live_execution_calldata(settings, candidate)?;
    let synthetic_simulation = SimulationResult {
        id: uuid::Uuid::new_v4(),
        opportunity_id: candidate.id,
        success: false,
        path_name: Some(candidate.path.name.clone()),
        token_in: Some(candidate.token_in),
        amount_in: Some(candidate.amount_in),
        expected_profit: Some(candidate.expected_profit),
        min_profit: Some(candidate.min_profit),
        simulated_profit: U256::ZERO,
        gas_estimate: None,
        block_number: Some(current_block),
        base_fee_per_gas: None,
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
        gas_cost_cap: None,
        gas_cost_expected: None,
        net_simulated_profit: None,
        revert_reason: Some("lazy approval preflight; simulation skipped".to_string()),
        calldata: calldata.clone(),
    };
    recorder
        .record_simulation(synthetic_simulation.clone())
        .await?;
    match tx_manager::submit_executor_approval(
        provider,
        fund_wallet,
        settings,
        executor,
        approval,
        nonce,
    )
    .await
    {
        Ok(submission) => {
            info!(
                candidate_id = %candidate.id,
                token = %approval.token,
                spender = %approval.spender,
                tx_hash = %submission.tx_hash,
                "executor approval submitted by fund EOA; current candidate skipped"
            );
            recorder
                .record_transaction(tx_manager::pending_tx_result(
                    candidate,
                    fund_wallet.address(),
                    &submission,
                ))
                .await?;
            let mut lane = eoa_store
                .get_lane_state(worker_wallet.address())
                .await?
                .map(|state| eoa_lane::EoaLane { state })
                .unwrap_or_else(|| eoa_lane::EoaLane::new(worker_wallet.address()));
            let arb_nonce = lane.state.local_nonce;
            match tx_manager::submit_candidate_unchecked(
                provider,
                worker_wallet,
                settings,
                candidate,
                Some(synthetic_simulation.id),
                &calldata,
                arb_nonce,
            )
            .await
            {
                Ok(arb_submission) => {
                    lane.mark_submitted(
                        candidate.id,
                        arb_submission.simulation_id,
                        arb_submission.tx_hash,
                        arb_submission.executor_contract,
                        arb_submission.nonce,
                        arb_submission.submitted_block,
                        arb_submission.gas_limit,
                        arb_submission.max_fee_per_gas,
                        arb_submission.max_priority_fee_per_gas,
                    );
                    eoa_store.set_lane_state(lane.state.clone()).await?;
                    recorder
                        .record_transaction(tx_manager::pending_tx_result(
                            candidate,
                            worker_wallet.address(),
                            &arb_submission,
                        ))
                        .await?;
                    info!(
                        candidate_id = %candidate.id,
                        approval_tx_hash = %submission.tx_hash,
                        arb_tx_hash = %arb_submission.tx_hash,
                        worker = %worker_wallet.address(),
                        "lazy approval submitted and arb tx fired without waiting"
                    );
                }
                Err(err) => {
                    warn!(
                        candidate_id = %candidate.id,
                        nonce = arb_nonce,
                        worker = %worker_wallet.address(),
                        error = %err,
                        "lazy approval submitted but follow-up arb tx failed before broadcast"
                    );
                    lane.mark_cooldown();
                    eoa_store.set_lane_state(lane.state.clone()).await?;
                    recorder
                        .record_transaction(TxResult {
                            opportunity_id: candidate.id,
                            simulation_id: Some(synthetic_simulation.id),
                            eoa: worker_wallet.address(),
                            tx_hash: None,
                            nonce: arb_nonce,
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
            Ok(Some(CandidateAction::Submitted))
        }
        Err(err) => {
            warn!(
                candidate_id = %candidate.id,
                token = %approval.token,
                spender = %approval.spender,
                error = %err,
                "executor approval admin tx failed"
            );
            recorder
                .record_transaction(TxResult {
                    opportunity_id: candidate.id,
                    simulation_id: Some(synthetic_simulation.id),
                    eoa: fund_wallet.address(),
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
            Ok(Some(CandidateAction::Simulated))
        }
    }
}

async fn submit_simulated_candidate<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    wallet: Option<&tx_manager::ExecutionWallet>,
    circuit_breaker: &ExecutionCircuitBreaker,
    candidate: &Candidate,
    simulation: &SimulationResult,
) -> Result<CandidateAction>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
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
    if circuit_breaker.is_halted(settings) {
        return Ok(CandidateAction::Skipped("circuit_breaker"));
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
    match tx_manager::submit_candidate(provider, wallet, settings, candidate, simulation, nonce)
        .await
    {
        Ok(submission) => {
            lane.mark_submitted(
                candidate.id,
                submission.simulation_id,
                submission.tx_hash,
                submission.executor_contract,
                submission.nonce,
                submission.submitted_block,
                submission.gas_limit,
                submission.max_fee_per_gas,
                submission.max_priority_fee_per_gas,
            );
            eoa_store.set_lane_state(lane.state.clone()).await?;
            recorder
                .record_transaction(tx_manager::pending_tx_result(
                    candidate,
                    wallet.address(),
                    &submission,
                ))
                .await?;
            Ok(CandidateAction::Submitted)
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
            Ok(CandidateAction::Simulated)
        }
    }
}

async fn select_cached_ready_worker<E>(
    eoa_store: &E,
    runtime: &ExecutionRuntime,
) -> Result<Option<tx_manager::ExecutionWallet>>
where
    E: EoaStateStore,
{
    for worker in &runtime.worker_wallets {
        let Some(state) = eoa_store.get_lane_state(worker.address()).await? else {
            continue;
        };
        if state.status == EoaLaneStatus::Idle && state.eth_balance >= runtime.worker_min_balance {
            debug!(
                worker = %worker.address(),
                balance = %state.eth_balance,
                local_nonce = state.local_nonce,
                confirmed_nonce = state.confirmed_nonce,
                "selected cached ready execution worker EOA"
            );
            return Ok(Some(worker.clone()));
        }
    }
    Ok(None)
}

enum CandidateAction {
    Skipped(&'static str),
    Simulated,
    Submitted,
}

#[derive(Debug, Clone)]
struct ExecutionCircuitBreaker {
    min_txs: u64,
    arb_receipts: u64,
    arb_successes: u64,
    halted: bool,
}

impl ExecutionCircuitBreaker {
    fn new(min_txs: u64) -> Self {
        Self {
            min_txs,
            arb_receipts: 0,
            arb_successes: 0,
            halted: false,
        }
    }

    fn observe_arb_receipt(&mut self, success: bool, settings: &Settings) {
        self.arb_receipts = self.arb_receipts.saturating_add(1);
        if success {
            self.arb_successes = self.arb_successes.saturating_add(1);
        }
        if self.arb_receipts < self.min_txs {
            return;
        }
        let success_rate_bps = self
            .arb_successes
            .saturating_mul(10_000)
            .checked_div(self.arb_receipts)
            .unwrap_or(0);
        if success_rate_bps < settings.execution_min_success_rate_bps {
            self.halted = true;
            warn!(
                arb_receipts = self.arb_receipts,
                arb_successes = self.arb_successes,
                success_rate_bps,
                min_success_rate_bps = settings.execution_min_success_rate_bps,
                "execution circuit breaker halted submissions due to low realized success rate"
            );
        }
    }

    fn is_halted(&self, settings: &Settings) -> bool {
        if self.halted {
            warn!(
                arb_receipts = self.arb_receipts,
                arb_successes = self.arb_successes,
                min_success_rate_bps = settings.execution_min_success_rate_bps,
                "execution submissions halted by circuit breaker"
            );
        }
        self.halted
    }
}

fn configured_fund_wallet(settings: &Settings) -> Result<Option<tx_manager::ExecutionWallet>> {
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

fn configured_worker_wallets(
    settings: &Settings,
    fund_wallet: Option<&tx_manager::ExecutionWallet>,
) -> Result<Vec<tx_manager::ExecutionWallet>> {
    let Some(private_key) = settings.eoa_private_key_1.as_deref() else {
        return Ok(Vec::new());
    };
    if !settings.execution_submit_enabled {
        return Ok(fund_wallet.cloned().into_iter().collect());
    }

    let pool_size = settings.execution_eoa_pool_size.max(1);
    let mut wallets = Vec::with_capacity(pool_size as usize);
    for index in 0..pool_size {
        wallets.push(derive_worker_wallet(private_key, settings.chain_id, index)?);
    }
    Ok(wallets)
}

fn derive_worker_wallet(
    fund_private_key: &str,
    chain_id: u64,
    index: u64,
) -> Result<tx_manager::ExecutionWallet> {
    for salt in 0u64..128 {
        let seed = format!("dexmoney-worker-eoa-v1:{fund_private_key}:{index}:{salt}");
        let digest = keccak256(seed.as_bytes());
        let private_key = format!("0x{}", hex::encode(digest.as_slice()));
        if let Ok(wallet) = tx_manager::ExecutionWallet::from_private_key(&private_key, chain_id) {
            return Ok(wallet);
        }
    }
    anyhow::bail!("failed to derive valid worker EOA private key for index {index}")
}

fn parse_config_u256(raw: Option<&str>, name: &str) -> Result<U256> {
    let value = raw.with_context(|| format!("{name} is not configured"))?;
    U256::from_str_radix(value.trim_start_matches("0x"), 10)
        .with_context(|| format!("invalid decimal U256 config value for {name}"))
}

async fn pop_fresh_candidates<C>(
    candidate_store: &C,
    current_block: u64,
    max_lag_blocks: u64,
) -> Result<Vec<base_arb_common::types::Candidate>>
where
    C: CandidateStore,
{
    let now = Utc::now();
    let mut expired = 0usize;
    let mut stale_by_block = 0usize;
    let mut candidates = Vec::new();
    let mut popped = 0usize;
    let mut min_fresh_lag = u64::MAX;
    let mut max_fresh_lag = 0u64;
    let mut min_stale_lag = u64::MAX;
    let mut max_stale_lag = 0u64;

    for candidate in candidate_store
        .pop_candidates(MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE)
        .await?
    {
        popped += 1;
        if candidate.is_expired(now) {
            expired += 1;
            continue;
        }
        let block_lag = current_block.saturating_sub(candidate.block_number);
        if block_lag > max_lag_blocks {
            stale_by_block += 1;
            min_stale_lag = min_stale_lag.min(block_lag);
            max_stale_lag = max_stale_lag.max(block_lag);
            continue;
        }
        min_fresh_lag = min_fresh_lag.min(block_lag);
        max_fresh_lag = max_fresh_lag.max(block_lag);
        candidates.push(candidate);
        if candidates.len() >= MAX_CANDIDATE_DRAIN_PER_CYCLE {
            break;
        }
    }

    if popped > 0 {
        let min_fresh_lag = if candidates.is_empty() {
            None
        } else {
            Some(min_fresh_lag)
        };
        let max_fresh_lag = if candidates.is_empty() {
            None
        } else {
            Some(max_fresh_lag)
        };
        let min_stale_lag = if stale_by_block == 0 {
            None
        } else {
            Some(min_stale_lag)
        };
        let max_stale_lag = if stale_by_block == 0 {
            None
        } else {
            Some(max_stale_lag)
        };
        info!(
            popped,
            fresh = candidates.len(),
            expired,
            stale_by_block,
            current_block,
            max_lag_blocks,
            ?min_fresh_lag,
            ?max_fresh_lag,
            ?min_stale_lag,
            ?max_stale_lag,
            "candidate queue drain summary"
        );
    }
    candidates.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));
    Ok(candidates)
}

async fn prepare_eoa_pool<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    runtime: &ExecutionRuntime,
    fund_wallet: &tx_manager::ExecutionWallet,
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<Option<tx_manager::ExecutionWallet>>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    reconcile_db_pending_transactions(recorder, provider, fund_wallet.address()).await?;

    for worker in &runtime.worker_wallets {
        reconcile_db_pending_transactions(recorder, provider, worker.address()).await?;
        let mut lane = eoa_store
            .get_lane_state(worker.address())
            .await?
            .map(|state| eoa_lane::EoaLane { state })
            .unwrap_or_else(|| eoa_lane::EoaLane::new(worker.address()));

        if lane.state.status == EoaLaneStatus::Pending {
            if let Some(success) =
                handle_pending_lane(&mut lane, eoa_store, recorder, provider, worker, settings)
                    .await?
            {
                circuit_breaker.observe_arb_receipt(success, settings);
            }
            continue;
        }

        sync_idle_lane(&mut lane, provider).await?;
        eoa_store.set_lane_state(lane.state.clone()).await?;
    }

    if circuit_breaker.is_halted(settings) {
        return Ok(None);
    }

    let admin_pending = admin_has_pending_nonce(provider, fund_wallet.address()).await?;
    let mut ready_worker = None;
    let mut incomplete_workers = 0u64;
    for worker in &runtime.worker_wallets {
        let lane = eoa_store
            .get_lane_state(worker.address())
            .await?
            .map(|state| eoa_lane::EoaLane { state })
            .unwrap_or_else(|| eoa_lane::EoaLane::new(worker.address()));
        if lane.state.status != EoaLaneStatus::Idle {
            continue;
        }

        if lane.state.eth_balance < runtime.worker_min_balance {
            incomplete_workers = incomplete_workers.saturating_add(1);
            if admin_pending {
                continue;
            }
            let top_up = runtime
                .worker_target_balance
                .saturating_sub(lane.state.eth_balance);
            if top_up.is_zero() {
                continue;
            }
            let fund_balance = provider.get_balance(fund_wallet.address()).await?;
            if fund_balance <= top_up {
                warn!(
                    fund = %fund_wallet.address(),
                    worker = %worker.address(),
                    fund_balance = %fund_balance,
                    top_up = %top_up,
                    "fund EOA balance is too low to top up worker"
                );
                return Ok(None);
            }
            let nonce = provider
                .get_transaction_count(fund_wallet.address(), true)
                .await?;
            tx_manager::submit_value_transfer(
                provider,
                fund_wallet,
                settings,
                worker.address(),
                top_up,
                nonce,
            )
            .await?;
            return Ok(None);
        }

        let mut missing_operator = None;
        for executor in &runtime.executor_contracts {
            if !tx_manager::operator_enabled(provider, *executor, worker.address()).await? {
                missing_operator = Some(*executor);
                break;
            }
        }
        if let Some(executor) = missing_operator {
            incomplete_workers = incomplete_workers.saturating_add(1);
            if admin_pending {
                continue;
            }
            let nonce = provider
                .get_transaction_count(fund_wallet.address(), true)
                .await?;
            tx_manager::submit_set_operator(
                provider,
                fund_wallet,
                settings,
                executor,
                worker.address(),
                nonce,
            )
            .await?;
            return Ok(None);
        }

        if ready_worker.is_none() {
            ready_worker = Some(worker.clone());
        }
    }

    if let Some(worker) = ready_worker {
        debug!(
            worker = %worker.address(),
            incomplete_workers,
            "selected execution worker EOA"
        );
        return Ok(Some(worker.clone()));
    }

    Ok(None)
}

async fn admin_has_pending_nonce(provider: &ChainProvider, address: Address) -> Result<bool> {
    let confirmed = provider.get_transaction_count(address, false).await?;
    let pending = provider.get_transaction_count(address, true).await?;
    Ok(pending > confirmed)
}

fn configured_executor_contracts(settings: &Settings) -> Vec<Address> {
    let mut executors = Vec::new();
    for executor in [
        settings.executor_contract_2hop,
        settings.executor_contract_multihop,
        settings.executor_contract,
    ]
    .into_iter()
    .flatten()
    {
        if executor != Address::ZERO && !executors.contains(&executor) {
            executors.push(executor);
        }
    }
    executors
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
) -> Result<Option<bool>>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let Some(tx_hash) = lane.state.pending_tx else {
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(None);
    };
    let Some(opportunity_id) = lane.state.pending_opportunity_id else {
        warn!(tx_hash = %tx_hash, "pending lane is missing opportunity id");
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(None);
    };
    let Some(nonce) = lane.state.pending_nonce else {
        warn!(tx_hash = %tx_hash, "pending lane is missing nonce");
        lane.mark_blocked();
        eoa_store.set_lane_state(lane.state.clone()).await?;
        return Ok(None);
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
            return Ok(None);
        }
        maybe_replace_pending_tx(lane, eoa_store, recorder, provider, wallet, settings).await?;
        debug!(tx_hash = %tx_hash, "pending tx has no receipt yet");
        return Ok(None);
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

    Ok(simulation_id.map(|_| receipt.success))
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
    let executor = lane
        .state
        .pending_executor_contract
        .or(settings.executor_contract)
        .unwrap_or(Address::ZERO);
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
        executor,
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
