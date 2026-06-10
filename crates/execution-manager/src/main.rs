mod eoa_lane;
mod simulator;
mod tx_manager;

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
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

    if settings.execution_submit_enabled {
        let fund_wallet = configured_fund_wallet(&settings)?;
        let worker_wallets = configured_worker_wallets(&settings, fund_wallet.as_ref())?;
        let workers = worker_wallets
            .iter()
            .map(|wallet| wallet.address().to_string())
            .collect::<Vec<_>>()
            .join(",");
        info!(
            fund = ?fund_wallet.as_ref().map(tx_manager::ExecutionWallet::address),
            workers,
            worker_count = worker_wallets.len(),
            "execution EOA pool configured"
        );
    }

    info!("execution-manager initialized");
    let mut ticker = interval(Duration::from_millis(500));
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
    min_simulated_profit_usdc: f64,
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<()>
where
    C: CandidateStore + FailureStore,
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let fund_wallet = configured_fund_wallet(settings)?;
    let worker_wallets = configured_worker_wallets(settings, fund_wallet.as_ref())?;
    let mut selected_wallet = None;

    if settings.execution_submit_enabled {
        let Some(fund_wallet) = fund_wallet.as_ref() else {
            anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
        };

        selected_wallet = prepare_eoa_pool(
            eoa_store,
            recorder,
            provider,
            settings,
            fund_wallet,
            &worker_wallets,
            circuit_breaker,
        )
        .await?;
        if selected_wallet.is_none() || circuit_breaker.is_halted(settings) {
            return Ok(());
        }
    }
    let operator = selected_wallet
        .as_ref()
        .map(tx_manager::ExecutionWallet::address)
        .unwrap_or(operator_address(settings, fund_wallet.as_ref())?);

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
            selected_wallet.as_ref(),
            fund_wallet.as_ref(),
            circuit_breaker,
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

async fn handle_candidate<C, E, R>(
    candidate_store: &C,
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    min_simulated_profit_usdc: f64,
    operator: Address,
    wallet: Option<&tx_manager::ExecutionWallet>,
    fund_wallet: Option<&tx_manager::ExecutionWallet>,
    circuit_breaker: &ExecutionCircuitBreaker,
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

    if settings.execution_submit_enabled {
        if wallet.is_none() {
            anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
        }
        let approvals = simulator::required_token_approvals(&candidate, settings)?;
        let missing_approvals =
            tx_manager::missing_approvals(provider, settings, &candidate, &approvals)
                .await
                .unwrap_or_else(|err| {
                    warn!(
                        candidate_id = %candidate.id,
                        error = %err,
                        "failed to check route allowances; trying route approvals before tx"
                    );
                    approvals
                });
        if !missing_approvals.is_empty() {
            let Some(fund_wallet) = fund_wallet else {
                anyhow::bail!("EOA_PRIVATE_KEY_1 is required for executor approval admin txs");
            };
            if admin_has_pending_nonce(provider, fund_wallet.address()).await? {
                debug!(
                    candidate_id = %candidate.id,
                    path = %candidate.path.name,
                    "candidate skipped because fund/admin EOA has a pending tx"
                );
                return Ok(CandidateAction::Skipped);
            }

            let executor = simulator::executor_for_candidate(settings, candidate)?;
            let nonce = provider
                .get_transaction_count(fund_wallet.address(), true)
                .await?;
            let approval = missing_approvals[0];
            let calldata = simulator::build_live_execution_calldata(settings, &candidate)?;
            let synthetic_simulation = base_arb_common::types::SimulationResult {
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
                            &candidate,
                            fund_wallet.address(),
                            &submission,
                        ))
                        .await?;
                    return Ok(CandidateAction::Submitted);
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
                    return Ok(CandidateAction::Simulated);
                }
            }
        }
    }

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
    if circuit_breaker.is_halted(settings) {
        return Ok(CandidateAction::Skipped);
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

fn parse_config_u256(raw: Option<&str>, name: &str) -> Result<U256> {
    let value = raw.with_context(|| format!("{name} is not configured"))?;
    U256::from_str_radix(value.trim_start_matches("0x"), 10)
        .with_context(|| format!("invalid decimal U256 config value for {name}"))
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

async fn prepare_eoa_pool<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    fund_wallet: &tx_manager::ExecutionWallet,
    worker_wallets: &[tx_manager::ExecutionWallet],
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<Option<tx_manager::ExecutionWallet>>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    reconcile_db_pending_transactions(recorder, provider, fund_wallet.address()).await?;

    for worker in worker_wallets {
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
    let worker_min_balance = parse_config_u256(
        settings.execution_worker_min_balance_wei.as_deref(),
        "execution_worker_min_balance_wei",
    )?;
    let worker_target_balance = parse_config_u256(
        settings.execution_worker_target_balance_wei.as_deref(),
        "execution_worker_target_balance_wei",
    )?
    .max(worker_min_balance);
    let executors = configured_executor_contracts(settings);

    for worker in worker_wallets {
        let lane = eoa_store
            .get_lane_state(worker.address())
            .await?
            .map(|state| eoa_lane::EoaLane { state })
            .unwrap_or_else(|| eoa_lane::EoaLane::new(worker.address()));
        if lane.state.status != EoaLaneStatus::Idle {
            continue;
        }

        if lane.state.eth_balance < worker_min_balance {
            if admin_pending {
                continue;
            }
            let top_up = worker_target_balance.saturating_sub(lane.state.eth_balance);
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
        for executor in &executors {
            if !tx_manager::operator_enabled(provider, *executor, worker.address()).await? {
                missing_operator = Some(*executor);
                break;
            }
        }
        if let Some(executor) = missing_operator {
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

        debug!(
            worker = %worker.address(),
            balance = %lane.state.eth_balance,
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
