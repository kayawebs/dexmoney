mod eoa_lane;
mod simulator;
mod tx_manager;

use alloy_primitives::{keccak256, Address, B256, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{Candidate, EoaLaneStatus, SimulationResult, TxResult, TxStatus};
use base_arb_storage::{
    postgres::PostgresStore, redis::RedisStore, CandidateStore, CurrentBlockStore, EoaStateStore,
    PendingTransactionStore, RecorderStore,
};
use chrono::Utc;
use std::collections::{hash_map::Entry, BTreeMap, HashMap, HashSet};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

const MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE: usize = 2_048;
const MAX_CANDIDATE_DRAIN_PER_CYCLE: usize = 128;
const MAX_SIMULATIONS_PER_CYCLE: usize = MAX_CANDIDATE_DRAIN_PER_CYCLE;
const CANDIDATE_POP_CHUNK_SIZE: usize = 128;
const SIMULATION_RECORD_QUEUE_CAPACITY: usize = 1_024;
const SIMULATION_RECORD_FLUSH_MS: u64 = 100;
const SIMULATION_RECORD_MAX_BATCH: usize = 512;
const SUBMITTED_CANDIDATE_LOCK_TTL_SECS: u64 = 300;
const WORKER_FUNDING_RECEIPT_POLL_MS: u64 = 250;
const WORKER_FUNDING_RECEIPT_ATTEMPTS: usize = 80;

#[derive(Debug, Clone)]
struct ExecutionRuntime {
    fund_wallet: Option<tx_manager::ExecutionWallet>,
    worker_wallets: Vec<tx_manager::ExecutionWallet>,
    simulation_operator: Option<Address>,
    worker_min_balance: U256,
    worker_target_balance: U256,
    executor_contracts: Vec<Address>,
    trusted_factories: simulator::TrustedFactorySet,
}

impl ExecutionRuntime {
    fn from_settings(
        settings: &Settings,
        trusted_factories: simulator::TrustedFactorySet,
    ) -> Result<Self> {
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
        )?;
        if worker_target_balance < worker_min_balance {
            anyhow::bail!(
                "execution_worker_target_balance_wei {} is below execution_worker_min_balance_wei {}",
                worker_target_balance,
                worker_min_balance
            );
        }
        Ok(Self {
            fund_wallet,
            worker_wallets,
            simulation_operator,
            worker_min_balance,
            worker_target_balance,
            executor_contracts: configured_executor_contracts(settings),
            trusted_factories,
        })
    }
}

#[derive(Clone)]
struct SimulationRecordQueue {
    sender: mpsc::Sender<Vec<SimulationResult>>,
}

impl SimulationRecordQueue {
    fn spawn(store: PostgresStore) -> Self {
        let (sender, mut receiver) =
            mpsc::channel::<Vec<SimulationResult>>(SIMULATION_RECORD_QUEUE_CAPACITY);
        tokio::spawn(async move {
            let mut pending = Vec::new();
            let mut flush_interval = interval(Duration::from_millis(SIMULATION_RECORD_FLUSH_MS));
            flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    maybe_simulations = receiver.recv() => {
                        let Some(mut simulations) = maybe_simulations else {
                            flush_simulation_records(&store, &mut pending).await;
                            break;
                        };
                        pending.append(&mut simulations);
                        if pending.len() >= SIMULATION_RECORD_MAX_BATCH {
                            flush_simulation_records(&store, &mut pending).await;
                        }
                    }
                    _ = flush_interval.tick(), if !pending.is_empty() => {
                        flush_simulation_records(&store, &mut pending).await;
                    }
                }
            }
        });
        Self { sender }
    }

    fn enqueue(&self, simulations: Vec<SimulationResult>) -> usize {
        if simulations.is_empty() {
            return 0;
        }
        match self.sender.try_send(simulations) {
            Ok(()) => 0,
            Err(mpsc::error::TrySendError::Full(simulations)) => {
                warn!(
                    dropped = simulations.len(),
                    "simulation record queue full; dropping diagnostic records"
                );
                simulations.len()
            }
            Err(mpsc::error::TrySendError::Closed(simulations)) => {
                warn!(
                    dropped = simulations.len(),
                    "simulation record queue closed; dropping diagnostic records"
                );
                simulations.len()
            }
        }
    }
}

async fn flush_simulation_records(store: &PostgresStore, pending: &mut Vec<SimulationResult>) {
    if pending.is_empty() {
        return;
    }
    let simulations = std::mem::take(pending);
    let count = simulations.len();
    if let Err(err) = store.record_simulations(simulations).await {
        warn!(
            count,
            error = %err,
            "failed to record simulations from background queue"
        );
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
    let factory_registry = postgres.trusted_factory_registry(settings.chain_id).await?;
    let trusted_factories =
        simulator::TrustedFactorySet::from_settings_and_registry(&settings, &factory_registry);
    info!(
        registry_records = factory_registry.len(),
        trusted_factories = trusted_factories.len(),
        "execution trusted factory registry loaded"
    );
    let runtime = ExecutionRuntime::from_settings(&settings, trusted_factories)?;
    let simulation_recorder = SimulationRecordQueue::spawn(postgres.clone());

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
        assert_submit_runtime_ready(&redis, &postgres, &provider, &settings, &runtime).await?;
    }
    let cleared_candidates = redis.clear_candidates().await?;
    if cleared_candidates > 0 {
        info!(
            cleared_candidates,
            "cleared stale candidate queue on execution-manager startup"
        );
    }

    info!("execution-manager initialized");
    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut circuit_breaker = ExecutionCircuitBreaker::new(settings.execution_failure_rate_min_txs);
    let mut maintenance = ExecutionMaintenance::default();

    loop {
        ticker.tick().await;
        run_execution_cycle(
            &redis,
            &redis,
            &postgres,
            &provider,
            &settings,
            &runtime,
            &simulation_recorder,
            settings.min_simulated_profit_usdc,
            &mut circuit_breaker,
            &mut maintenance,
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
    simulation_recorder: &SimulationRecordQueue,
    min_simulated_profit_usdc: f64,
    circuit_breaker: &mut ExecutionCircuitBreaker,
    maintenance: &mut ExecutionMaintenance,
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
    let selected_wallet = if settings.execution_submit_enabled {
        if circuit_breaker.is_halted(settings) {
            anyhow::bail!("execution circuit breaker halted; no candidates were popped");
        }
        reconcile_pending_worker_lanes(
            eoa_store,
            recorder,
            provider,
            settings,
            runtime,
            circuit_breaker,
        )
        .await?;
        if circuit_breaker.is_halted(settings) {
            anyhow::bail!(
                "execution circuit breaker halted after pending lane reconciliation; no candidates were popped"
            );
        }
        match select_cached_ready_worker(eoa_store, runtime).await? {
            Some(wallet) => Some(wallet),
            None => {
                reconcile_worker_lanes(
                    eoa_store,
                    recorder,
                    provider,
                    settings,
                    runtime,
                    circuit_breaker,
                )
                .await?;
                if circuit_breaker.is_halted(settings) {
                    anyhow::bail!(
                        "execution circuit breaker halted after lane reconciliation; no candidates were popped"
                    );
                }
                Some(
                    select_cached_ready_worker(eoa_store, runtime)
                        .await?
                        .with_context(|| {
                            "no ready execution worker EOA after lane reconciliation; no candidates were popped"
                        })?,
                )
            }
        }
    } else {
        None
    };
    let candidates =
        pop_fresh_candidates(candidate_store, current_block, max_candidate_lag_blocks).await?;
    let drained_candidate_count = candidates.len();
    let (mut candidates, coalesced_duplicate_candidates) =
        coalesce_candidates_by_execution_key(candidates);
    let skipped_submitted_duplicates = if settings.execution_submit_enabled {
        maintenance
            .submitted_candidate_keys
            .prune_before(current_block.saturating_sub(max_candidate_lag_blocks));
        let before = candidates.len();
        candidates.retain(|candidate| {
            !maintenance
                .submitted_candidate_keys
                .contains_candidate(candidate)
        });
        before.saturating_sub(candidates.len())
    } else {
        0
    };
    if coalesced_duplicate_candidates > 0 || skipped_submitted_duplicates > 0 {
        info!(
            drained_candidate_count,
            candidate_count = candidates.len(),
            coalesced_duplicate_candidates,
            skipped_submitted_duplicates,
            submitted_dedup_cache_size = maintenance.submitted_candidate_keys.len(),
            current_block,
            "execution candidate dedup summary"
        );
    }
    if candidates.is_empty() {
        debug!("no candidate available");
        return Ok(());
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
    let simulation_context = maintenance
        .simulation_context(provider, settings, current_block)
        .await;
    if !settings.execution_submit_enabled {
        return run_dry_run_simulation_batch(
            simulation_recorder,
            provider,
            settings,
            &runtime.trusted_factories,
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
        candidate_store,
        eoa_store,
        recorder,
        simulation_recorder,
        provider,
        settings,
        &runtime.trusted_factories,
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
        &mut maintenance.approved_allowances,
        &mut maintenance.submitted_candidate_keys,
    )
    .await
}

#[derive(Default)]
struct ExecutionMaintenance {
    approved_allowances: HashSet<(Address, Address, Address)>,
    simulation_context: Option<(u64, simulator::SimulationContext)>,
    submitted_candidate_keys: SubmittedCandidateKeys,
}

impl ExecutionMaintenance {
    async fn simulation_context(
        &mut self,
        provider: &ChainProvider,
        settings: &Settings,
        current_block: u64,
    ) -> simulator::SimulationContext {
        if let Some((block, context)) = &self.simulation_context {
            if *block == current_block {
                return context.clone();
            }
        }
        let context = simulator::SimulationContext::load(provider, settings, current_block).await;
        self.simulation_context = Some((current_block, context.clone()));
        context
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CandidateDedupKey {
    block_number: u64,
    path_name: String,
    token_in: Address,
    amount_in: U256,
}

impl CandidateDedupKey {
    fn from_candidate(candidate: &Candidate) -> Self {
        Self {
            block_number: candidate.block_number,
            path_name: candidate.path.name.clone(),
            token_in: candidate.token_in,
            amount_in: candidate.amount_in,
        }
    }
}

#[derive(Debug, Default)]
struct SubmittedCandidateKeys {
    keys: HashSet<CandidateDedupKey>,
}

impl SubmittedCandidateKeys {
    fn contains_candidate(&self, candidate: &Candidate) -> bool {
        self.keys
            .contains(&CandidateDedupKey::from_candidate(candidate))
    }

    fn insert_candidate(&mut self, candidate: &Candidate) -> bool {
        self.keys
            .insert(CandidateDedupKey::from_candidate(candidate))
    }

    fn prune_before(&mut self, min_block_number: u64) {
        self.keys.retain(|key| key.block_number >= min_block_number);
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

fn coalesce_candidates_by_execution_key(candidates: Vec<Candidate>) -> (Vec<Candidate>, usize) {
    let mut best_by_key = HashMap::<CandidateDedupKey, Candidate>::new();
    let mut coalesced = 0usize;
    for candidate in candidates {
        let key = CandidateDedupKey::from_candidate(&candidate);
        match best_by_key.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
            Entry::Occupied(mut entry) => {
                coalesced += 1;
                if candidate.expected_profit > entry.get().expected_profit {
                    entry.insert(candidate);
                }
            }
        }
    }

    let mut candidates = best_by_key.into_values().collect::<Vec<_>>();
    candidates.sort_by(|a, b| b.expected_profit.cmp(&a.expected_profit));
    (candidates, coalesced)
}

async fn run_dry_run_simulation_batch(
    simulation_recorder: &SimulationRecordQueue,
    provider: &ChainProvider,
    settings: &Settings,
    trusted_factories: &simulator::TrustedFactorySet,
    operator: Address,
    candidates: Vec<Candidate>,
    min_simulated_profit_usdc: f64,
    simulation_context: simulator::SimulationContext,
    current_block: u64,
    max_candidate_lag_blocks: u64,
    batch_started: Instant,
) -> Result<()> {
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
            let trusted_factories = trusted_factories.clone();
            let simulation_context = simulation_context.clone();
            join_set.spawn(async move {
                simulator::simulate(
                    &provider,
                    &settings,
                    &trusted_factories,
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

    let simulation_record_queue_dropped = simulation_recorder.enqueue(simulation_records);

    info!(
        candidate_count,
        current_block,
        max_candidate_lag_blocks,
        simulated,
        simulation_successes,
        simulation_failures,
        join_errors,
        concurrency,
        simulation_record_queue_dropped,
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
    submission_lock_store: &impl CandidateStore,
    eoa_store: &E,
    recorder: &R,
    simulation_recorder: &SimulationRecordQueue,
    provider: &ChainProvider,
    settings: &Settings,
    trusted_factories: &simulator::TrustedFactorySet,
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
    approved_allowances: &mut HashSet<(Address, Address, Address)>,
    submitted_candidate_keys: &mut SubmittedCandidateKeys,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    let candidate_count = candidates.len();
    let mut skipped_by_reason: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut approval_ready = Vec::new();
    let mut preflight_simulated = 0usize;
    recorder.record_opportunities(candidates.clone()).await?;

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
            simulation_recorder,
            provider,
            settings,
            trusted_factories,
            wallet,
            fund_wallet,
            &candidate,
            current_block,
            approved_allowances,
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
                        approved_allowance_cache_size = approved_allowances.len(),
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
            let trusted_factories = trusted_factories.clone();
            let simulation_context = simulation_context.clone();
            join_set.spawn(async move {
                let simulation = simulator::simulate(
                    &provider,
                    &settings,
                    &trusted_factories,
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
                let LiveSimulationOutcome {
                    candidate,
                    simulation,
                } = outcome;
                if simulation.success {
                    simulation_successes += 1;
                    simulation_records.push(simulation.clone());
                    successful.push(LiveSimulationOutcome {
                        candidate,
                        simulation,
                    });
                } else {
                    simulation_failures += 1;
                    simulation_records.push(simulation);
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

    let simulation_record_queue_dropped = simulation_recorder.enqueue(simulation_records);

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
            trusted_factories,
            wallet,
            circuit_breaker,
            &outcome.candidate,
            &outcome.simulation,
            submission_lock_store,
        )
        .await?
        {
            CandidateAction::Submitted => {
                submitted_candidate_keys.insert_candidate(&outcome.candidate);
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
        simulation_record_queue_dropped,
        submitted,
        skipped = skipped_by_reason.values().sum::<usize>(),
        approved_allowance_cache_size = approved_allowances.len(),
        submitted_dedup_cache_size = submitted_candidate_keys.len(),
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
    _eoa_store: &E,
    recorder: &R,
    simulation_recorder: &SimulationRecordQueue,
    provider: &ChainProvider,
    settings: &Settings,
    trusted_factories: &simulator::TrustedFactorySet,
    _wallet: Option<&tx_manager::ExecutionWallet>,
    fund_wallet: Option<&tx_manager::ExecutionWallet>,
    candidate: &Candidate,
    current_block: u64,
    approved_cache: &mut HashSet<(Address, Address, Address)>,
) -> Result<Option<CandidateAction>>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    if !settings.execution_submit_enabled && !settings.execution_auto_approve_enabled {
        return Ok(None);
    }

    let preflight_calldata =
        match simulator::build_live_execution_calldata(settings, trusted_factories, candidate) {
            Ok(calldata) => calldata,
            Err(err) => {
                let reason = format!("executor preflight rejected: {err:#}");
                warn!(
                    candidate_id = %candidate.id,
                    path = %candidate.path.name,
                    reason = %reason,
                    "candidate skipped before approval preflight because it cannot be encoded"
                );
                enqueue_preflight_failure_simulation(
                    simulation_recorder,
                    candidate,
                    current_block,
                    reason,
                    Vec::new(),
                );
                return Ok(Some(CandidateAction::Skipped("preflight_unencodable")));
            }
        };

    let approvals = match simulator::required_token_approvals(
        candidate,
        settings,
        trusted_factories,
    ) {
        Ok(approvals) => approvals,
        Err(err) => {
            let reason = format!("executor approval preflight rejected: {err:#}");
            warn!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                reason = %reason,
                "candidate skipped before approval preflight because approvals cannot be derived"
            );
            enqueue_preflight_failure_simulation(
                simulation_recorder,
                candidate,
                current_block,
                reason,
                preflight_calldata,
            );
            return Ok(Some(CandidateAction::Skipped("preflight_unencodable")));
        }
    };
    let approval_count = approvals.len();
    let missing_approvals = tx_manager::missing_approvals_cached(
        provider,
        settings,
        trusted_factories,
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
    if approval_count > 0 {
        info!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            approvals = approval_count,
            missing_approvals = missing_approvals.len(),
            submit_enabled = settings.execution_submit_enabled,
            auto_approve_enabled = settings.execution_auto_approve_enabled,
            "executor approval preflight checked route allowances"
        );
    }
    if missing_approvals.is_empty() {
        return Ok(None);
    }

    let Some(fund_wallet) = fund_wallet else {
        anyhow::bail!("EOA_PRIVATE_KEY_1 is required for executor approval admin txs");
    };
    if admin_has_pending_nonce(provider, fund_wallet.address()).await? {
        info!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            fund = %fund_wallet.address(),
            missing_approvals = missing_approvals.len(),
            "candidate skipped because fund/admin EOA has a pending tx"
        );
        return Ok(Some(CandidateAction::Skipped("admin_pending_approval")));
    }

    let executor = match simulator::executor_for_candidate(settings, trusted_factories, candidate) {
        Ok(executor) => executor,
        Err(err) => {
            let reason = format!("executor selection preflight rejected: {err:#}");
            warn!(
                candidate_id = %candidate.id,
                path = %candidate.path.name,
                reason = %reason,
                "candidate skipped before approval preflight because executor cannot be selected"
            );
            enqueue_preflight_failure_simulation(
                simulation_recorder,
                candidate,
                current_block,
                reason,
                preflight_calldata,
            );
            return Ok(Some(CandidateAction::Skipped("preflight_unencodable")));
        }
    };
    let nonce = provider
        .get_transaction_count(fund_wallet.address(), true)
        .await?;
    let approval = missing_approvals[0];
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
        calldata: preflight_calldata.clone(),
    };
    let simulation_record_queue_dropped =
        simulation_recorder.enqueue(vec![synthetic_simulation.clone()]);
    if simulation_record_queue_dropped > 0 {
        warn!(
            candidate_id = %candidate.id,
            simulation_record_queue_dropped,
            "lazy approval synthetic simulation record dropped"
        );
    }
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
                submit_enabled = settings.execution_submit_enabled,
                missing_approvals = missing_approvals.len(),
                "executor approval submitted by fund EOA; skipping arb tx until allowance is live"
            );
            Ok(Some(CandidateAction::Simulated))
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
                    simulation_id: None,
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

fn enqueue_preflight_failure_simulation(
    simulation_recorder: &SimulationRecordQueue,
    candidate: &Candidate,
    current_block: u64,
    reason: String,
    calldata: Vec<u8>,
) {
    let simulation = SimulationResult {
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
        revert_reason: Some(reason),
        calldata,
    };
    let dropped = simulation_recorder.enqueue(vec![simulation]);
    if dropped > 0 {
        warn!(
            candidate_id = %candidate.id,
            simulation_record_queue_dropped = dropped,
            "preflight failure simulation record dropped"
        );
    }
}

async fn submit_simulated_candidate<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    trusted_factories: &simulator::TrustedFactorySet,
    wallet: Option<&tx_manager::ExecutionWallet>,
    circuit_breaker: &ExecutionCircuitBreaker,
    candidate: &Candidate,
    simulation: &SimulationResult,
    submission_lock_store: &impl CandidateStore,
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
    let submission_lock_key = submitted_candidate_lock_key(candidate);
    if !submission_lock_store
        .try_acquire_submission_lock(&submission_lock_key, SUBMITTED_CANDIDATE_LOCK_TTL_SECS)
        .await?
    {
        info!(
            candidate_id = %candidate.id,
            path = %candidate.path.name,
            candidate_block = candidate.block_number,
            token_in = %candidate.token_in,
            amount_in = %candidate.amount_in,
            submission_lock_key,
            "candidate skipped because submission lock is already held"
        );
        return Ok(CandidateAction::Skipped("submission_lock"));
    }
    match tx_manager::submit_candidate(
        provider,
        wallet,
        settings,
        trusted_factories,
        candidate,
        simulation,
        nonce,
    )
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

fn submitted_candidate_lock_key(candidate: &Candidate) -> String {
    let key = CandidateDedupKey::from_candidate(candidate);
    let payload = format!(
        "{}:{}:{:#x}:{}",
        key.block_number, key.path_name, key.token_in, key.amount_in
    );
    hex::encode(keccak256(payload.as_bytes()).as_slice())
}

async fn assert_submit_runtime_ready<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    runtime: &ExecutionRuntime,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    if runtime.fund_wallet.is_none() {
        anyhow::bail!("EOA_PRIVATE_KEY_1 is required when EXECUTION_SUBMIT_ENABLED=true");
    }
    if runtime.worker_wallets.is_empty() {
        anyhow::bail!("execution worker EOA pool is empty");
    }
    if runtime.executor_contracts.is_empty() {
        anyhow::bail!("no executor contract configured for live submission");
    }

    let mut circuit_breaker = ExecutionCircuitBreaker::new(settings.execution_failure_rate_min_txs);
    reconcile_worker_lanes(
        eoa_store,
        recorder,
        provider,
        settings,
        runtime,
        &mut circuit_breaker,
    )
    .await?;
    if circuit_breaker.is_halted(settings) {
        anyhow::bail!("execution circuit breaker halted during startup lane reconciliation");
    }

    let mut ready_workers = 0usize;
    let mut not_ready = Vec::new();
    for worker in &runtime.worker_wallets {
        let address = worker.address();
        let Some(state) = eoa_store.get_lane_state(address).await? else {
            not_ready.push(format!("{address:#x}: missing lane state"));
            continue;
        };
        if state.status != EoaLaneStatus::Idle {
            not_ready.push(format!("{address:#x}: status={:?}", state.status));
            continue;
        }
        if state.eth_balance < runtime.worker_min_balance {
            not_ready.push(format!(
                "{address:#x}: low eth balance {} < {}",
                state.eth_balance, runtime.worker_min_balance
            ));
            continue;
        }
        let mut missing_operator = None;
        for executor in &runtime.executor_contracts {
            if !tx_manager::operator_enabled(provider, *executor, address).await? {
                missing_operator = Some(*executor);
                break;
            }
        }
        if let Some(executor) = missing_operator {
            not_ready.push(format!(
                "{address:#x}: not operator for executor {executor:#x}"
            ));
            continue;
        }
        ready_workers += 1;
    }

    if ready_workers == 0 {
        anyhow::bail!(
            "no ready execution worker EOA at startup: {}",
            not_ready.join("; ")
        );
    }
    info!(
        ready_workers,
        worker_count = runtime.worker_wallets.len(),
        "execution worker readiness verified"
    );
    Ok(())
}

async fn reconcile_worker_lanes<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    runtime: &ExecutionRuntime,
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    for worker in &runtime.worker_wallets {
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
            if lane.state.status == EoaLaneStatus::Pending {
                continue;
            }
        }

        sync_idle_lane(&mut lane, provider).await?;
        fund_worker_lane_if_needed(&mut lane, provider, settings, runtime).await?;
        eoa_store.set_lane_state(lane.state.clone()).await?;
    }
    Ok(())
}

async fn reconcile_pending_worker_lanes<E, R>(
    eoa_store: &E,
    recorder: &R,
    provider: &ChainProvider,
    settings: &Settings,
    runtime: &ExecutionRuntime,
    circuit_breaker: &mut ExecutionCircuitBreaker,
) -> Result<()>
where
    E: EoaStateStore,
    R: RecorderStore + PendingTransactionStore,
{
    for worker in &runtime.worker_wallets {
        let Some(state) = eoa_store.get_lane_state(worker.address()).await? else {
            continue;
        };
        if state.status != EoaLaneStatus::Pending {
            continue;
        }
        let mut lane = eoa_lane::EoaLane { state };
        if let Some(success) =
            handle_pending_lane(&mut lane, eoa_store, recorder, provider, worker, settings).await?
        {
            circuit_breaker.observe_arb_receipt(success, settings);
        }
    }
    Ok(())
}

async fn fund_worker_lane_if_needed(
    lane: &mut eoa_lane::EoaLane,
    provider: &ChainProvider,
    settings: &Settings,
    runtime: &ExecutionRuntime,
) -> Result<()> {
    if lane.state.status != EoaLaneStatus::Idle {
        return Ok(());
    }
    if lane.state.eth_balance >= runtime.worker_min_balance {
        return Ok(());
    }
    let Some(fund_wallet) = runtime.fund_wallet.as_ref() else {
        anyhow::bail!(
            "worker {:#x} has low balance {} < {}, but fund wallet is not configured",
            lane.state.address,
            lane.state.eth_balance,
            runtime.worker_min_balance
        );
    };
    if fund_wallet.address() == lane.state.address {
        anyhow::bail!(
            "worker {:#x} has low balance {} < {}, and is also the fund wallet",
            lane.state.address,
            lane.state.eth_balance,
            runtime.worker_min_balance
        );
    }

    let value = runtime
        .worker_target_balance
        .saturating_sub(lane.state.eth_balance);
    if value.is_zero() {
        return Ok(());
    }
    let fund_balance = provider.get_balance(fund_wallet.address()).await?;
    if fund_balance <= value {
        anyhow::bail!(
            "fund wallet {:#x} balance {} is not enough to fund worker {:#x} value {}",
            fund_wallet.address(),
            fund_balance,
            lane.state.address,
            value
        );
    }
    let nonce = provider
        .get_transaction_count(fund_wallet.address(), true)
        .await?;
    let submission = tx_manager::submit_native_transfer(
        provider,
        fund_wallet,
        settings,
        lane.state.address,
        value,
        nonce,
    )
    .await?;
    wait_for_worker_funding_receipt(provider, submission.tx_hash).await?;
    sync_idle_lane(lane, provider).await?;
    info!(
        worker = %lane.state.address,
        balance = %lane.state.eth_balance,
        target_balance = %runtime.worker_target_balance,
        min_balance = %runtime.worker_min_balance,
        tx_hash = %submission.tx_hash,
        "worker funding confirmed"
    );
    Ok(())
}

async fn wait_for_worker_funding_receipt(provider: &ChainProvider, tx_hash: B256) -> Result<()> {
    for _ in 0..WORKER_FUNDING_RECEIPT_ATTEMPTS {
        if let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? {
            if receipt.success {
                return Ok(());
            }
            anyhow::bail!("worker funding tx {tx_hash:#x} reverted");
        }
        sleep(Duration::from_millis(WORKER_FUNDING_RECEIPT_POLL_MS)).await;
    }
    anyhow::bail!("timed out waiting for worker funding tx {tx_hash:#x} receipt")
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
    let first_fresh_block = current_block.saturating_sub(max_lag_blocks);
    let pruned_stale_by_score = candidate_store
        .prune_candidates_before_block(first_fresh_block)
        .await?;

    while candidates.len() < MAX_CANDIDATE_DRAIN_PER_CYCLE
        && popped < MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE
    {
        let remaining_drain = MAX_EXPIRED_CANDIDATE_DRAIN_PER_CYCLE - popped;
        let remaining_fresh = MAX_CANDIDATE_DRAIN_PER_CYCLE - candidates.len();
        let pop_limit = CANDIDATE_POP_CHUNK_SIZE
            .min(remaining_drain)
            .min(remaining_fresh);
        if pop_limit == 0 {
            break;
        }

        let popped_candidates = candidate_store.pop_candidates(pop_limit).await?;
        if popped_candidates.is_empty() {
            break;
        }

        for candidate in popped_candidates {
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
        }
    }

    if popped > 0 || pruned_stale_by_score > 0 {
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
            pruned_stale_by_score,
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

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use base_arb_common::types::{ArbPath, Candidate, OpportunityStatus};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use super::{
        coalesce_candidates_by_execution_key, submitted_candidate_lock_key, SubmittedCandidateKeys,
    };

    fn candidate(
        block_number: u64,
        path_name: &str,
        amount_in: u64,
        expected_profit: u64,
    ) -> Candidate {
        Candidate {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(1),
            block_number,
            strategy: "test".to_string(),
            token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            amount_in: U256::from(amount_in),
            expected_amount_out: U256::from(amount_in + expected_profit),
            expected_profit: U256::from(expected_profit),
            min_profit: U256::from(5_000u64),
            price_impact_bps: 1,
            path: ArbPath {
                name: path_name.to_string(),
                steps: Vec::new(),
                diagnostics: None,
            },
            status: OpportunityStatus::Created,
        }
    }

    #[test]
    fn coalesce_candidates_keeps_highest_profit_for_same_execution_key() {
        let low = candidate(100, "same-path", 1_000_000, 10_000);
        let high = candidate(100, "same-path", 1_000_000, 20_000);
        let different_amount = candidate(100, "same-path", 2_000_000, 15_000);

        let (coalesced, duplicates) =
            coalesce_candidates_by_execution_key(vec![low, different_amount, high.clone()]);

        assert_eq!(duplicates, 1);
        assert_eq!(coalesced.len(), 2);
        assert_eq!(coalesced[0].id, high.id);
        assert_eq!(coalesced[0].expected_profit, U256::from(20_000u64));
    }

    #[test]
    fn submitted_candidate_keys_match_same_block_path_token_and_amount() {
        let first = candidate(100, "same-path", 1_000_000, 10_000);
        let same_key_higher_profit = candidate(100, "same-path", 1_000_000, 20_000);
        let next_block = candidate(101, "same-path", 1_000_000, 20_000);

        let mut keys = SubmittedCandidateKeys::default();
        assert!(keys.insert_candidate(&first));
        assert!(keys.contains_candidate(&same_key_higher_profit));
        assert!(!keys.contains_candidate(&next_block));

        keys.prune_before(101);
        assert_eq!(keys.len(), 0);
        assert!(!keys.contains_candidate(&same_key_higher_profit));
    }

    #[test]
    fn submitted_candidate_lock_key_matches_execution_key() {
        let first = candidate(100, "same-path", 1_000_000, 10_000);
        let same_key_higher_profit = candidate(100, "same-path", 1_000_000, 20_000);
        let different_amount = candidate(100, "same-path", 2_000_000, 20_000);
        let next_block = candidate(101, "same-path", 1_000_000, 20_000);

        assert_eq!(
            submitted_candidate_lock_key(&first),
            submitted_candidate_lock_key(&same_key_higher_profit)
        );
        assert_ne!(
            submitted_candidate_lock_key(&first),
            submitted_candidate_lock_key(&different_amount)
        );
        assert_ne!(
            submitted_candidate_lock_key(&first),
            submitted_candidate_lock_key(&next_block)
        );
    }
}
