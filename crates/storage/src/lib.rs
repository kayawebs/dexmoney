pub mod postgres;
pub mod redis;
pub mod schema;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;

use base_arb_chain::events::DexEvent;
use base_arb_common::types::{
    Candidate, EoaLaneState, PoolState, SimulationResult, TickState, TokenPairSearchConfig,
    TxResult,
};

#[async_trait]
pub trait PoolStateStore: Send + Sync {
    async fn set_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()>;
    async fn set_pool_states(&self, pool_states: Vec<PoolState>) -> anyhow::Result<()> {
        for pool_state in pool_states {
            self.set_pool_state(pool_state).await?;
        }
        Ok(())
    }
    async fn get_pool_state(&self, address: Address) -> anyhow::Result<Option<PoolState>>;
    async fn get_pool_states(
        &self,
        addresses: &[Address],
    ) -> anyhow::Result<Vec<(Address, Option<PoolState>)>>;
    async fn all_pool_states(&self) -> anyhow::Result<Vec<PoolState>>;
}

#[async_trait]
pub trait PoolChangeStore: Send + Sync {
    async fn mark_changed_pools(&self, pools: Vec<Address>) -> anyhow::Result<()>;
    async fn drain_changed_pools(&self) -> anyhow::Result<Vec<Address>>;
}

#[async_trait]
pub trait TickChangeStore: Send + Sync {
    async fn mark_tick_changed_pools(&self, pools: Vec<Address>) -> anyhow::Result<()>;
    async fn drain_tick_changed_pools(&self) -> anyhow::Result<Vec<Address>>;
}

#[async_trait]
pub trait TickRepairStore: Send + Sync {
    async fn mark_tick_repair_pools(&self, pools: Vec<Address>) -> anyhow::Result<()>;
    async fn drain_tick_repair_pools(&self, limit: usize) -> anyhow::Result<Vec<Address>>;
}

#[async_trait]
pub trait CurrentBlockStore: Send + Sync {
    async fn set_current_block(&self, block_number: u64) -> anyhow::Result<()>;
    async fn get_current_block(&self) -> anyhow::Result<Option<u64>>;
}

#[async_trait]
pub trait PoolRuntimeStore: PoolStateStore + PoolChangeStore + CurrentBlockStore {
    async fn publish_pool_states(
        &self,
        current_block: u64,
        pool_states: Vec<PoolState>,
        changed_pools: Vec<Address>,
    ) -> anyhow::Result<()> {
        self.set_current_block(current_block).await?;
        self.set_pool_states(pool_states).await?;
        self.mark_changed_pools(changed_pools).await?;
        Ok(())
    }
}

#[async_trait]
pub trait TickStateStore: Send + Sync {
    async fn set_tick_state(&self, tick_state: TickState) -> anyhow::Result<()>;
    async fn set_tick_states(&self, tick_states: Vec<TickState>) -> anyhow::Result<()>;
    async fn replace_pool_ticks(
        &self,
        pool: Address,
        tick_states: Vec<TickState>,
    ) -> anyhow::Result<()>;
    async fn get_pool_ticks(&self, pool: Address) -> anyhow::Result<Vec<TickState>>;
    async fn get_pool_ticks_many(
        &self,
        pools: &[Address],
    ) -> anyhow::Result<HashMap<Address, Vec<TickState>>>;
}

#[async_trait]
pub trait CandidateStore: Send + Sync {
    async fn push_candidate(&self, candidate: Candidate) -> anyhow::Result<()>;
    async fn push_candidates(&self, candidates: &[Candidate]) -> anyhow::Result<()> {
        for candidate in candidates {
            self.push_candidate(candidate.clone()).await?;
        }
        Ok(())
    }
    async fn prune_candidates_before_block(&self, _block_number: u64) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn pop_candidate(&self) -> anyhow::Result<Option<Candidate>>;
    async fn pop_candidates(&self, limit: usize) -> anyhow::Result<Vec<Candidate>>;
    async fn try_acquire_submission_lock(
        &self,
        _key: &str,
        _ttl_secs: u64,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}

#[async_trait]
pub trait FailureStore: Send + Sync {
    async fn mark_failure_key(&self, key: &str, ttl_secs: u64) -> anyhow::Result<()>;
    async fn has_failure_key(&self, key: &str) -> anyhow::Result<bool>;
}

#[async_trait]
pub trait RecorderStore: Send + Sync {
    async fn record_dex_event(&self, event: DexEvent) -> anyhow::Result<()>;
    async fn record_dex_events(&self, events: Vec<DexEvent>) -> anyhow::Result<()> {
        for event in events {
            self.record_dex_event(event).await?;
        }
        Ok(())
    }
    async fn record_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()>;
    async fn record_pool_state_with_source(
        &self,
        pool_state: PoolState,
        source: &str,
    ) -> anyhow::Result<()> {
        let _ = source;
        self.record_pool_state(pool_state).await
    }
    async fn record_pool_states_with_source(
        &self,
        pool_states: Vec<PoolState>,
        source: &str,
    ) -> anyhow::Result<()> {
        for pool_state in pool_states {
            self.record_pool_state_with_source(pool_state, source)
                .await?;
        }
        Ok(())
    }
    async fn record_opportunity(&self, candidate: Candidate) -> anyhow::Result<()>;
    async fn record_opportunities(&self, candidates: Vec<Candidate>) -> anyhow::Result<()> {
        for candidate in candidates {
            self.record_opportunity(candidate).await?;
        }
        Ok(())
    }
    async fn record_simulation(&self, simulation: SimulationResult) -> anyhow::Result<()>;
    async fn record_simulations(&self, simulations: Vec<SimulationResult>) -> anyhow::Result<()> {
        for simulation in simulations {
            self.record_simulation(simulation).await?;
        }
        Ok(())
    }
    async fn record_transaction(&self, tx: TxResult) -> anyhow::Result<()>;
}

#[async_trait]
pub trait PairSearchConfigStore: Send + Sync {
    async fn enabled_pair_search_configs(&self) -> anyhow::Result<Vec<TokenPairSearchConfig>>;
}

#[async_trait]
pub trait EoaStateStore: Send + Sync {
    async fn set_lane_state(&self, lane: EoaLaneState) -> anyhow::Result<()>;
    async fn get_lane_state(&self, address: Address) -> anyhow::Result<Option<EoaLaneState>>;
}

#[derive(Debug, Clone)]
pub struct PendingTransactionRecord {
    pub opportunity_id: uuid::Uuid,
    pub simulation_id: Option<uuid::Uuid>,
    pub eoa: Address,
    pub tx_hash: B256,
    pub nonce: u64,
}

#[async_trait]
pub trait PendingTransactionStore: Send + Sync {
    async fn pending_transactions_for_eoa(
        &self,
        eoa: Address,
        limit: i64,
    ) -> anyhow::Result<Vec<PendingTransactionRecord>>;
    async fn simulation_calldata(
        &self,
        simulation_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Vec<u8>>>;
}

#[derive(Clone, Default)]
pub struct InMemoryStores {
    pool_states: Arc<Mutex<BTreeMap<Address, PoolState>>>,
    candidates: Arc<Mutex<VecDeque<Candidate>>>,
    failures: Arc<Mutex<BTreeMap<String, DateTime<Utc>>>>,
    opportunities: Arc<Mutex<Vec<Candidate>>>,
    simulations: Arc<Mutex<Vec<SimulationResult>>>,
    transactions: Arc<Mutex<Vec<TxResult>>>,
    lanes: Arc<Mutex<BTreeMap<Address, EoaLaneState>>>,
    events: Arc<Mutex<Vec<DexEvent>>>,
    pool_snapshots: Arc<Mutex<Vec<PoolState>>>,
    ticks: Arc<Mutex<BTreeMap<(Address, i32), TickState>>>,
    changed_pools: Arc<Mutex<VecDeque<Address>>>,
    tick_changed_pools: Arc<Mutex<VecDeque<Address>>>,
    current_block: Arc<Mutex<Option<u64>>>,
}

impl InMemoryStores {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PairSearchConfigStore for InMemoryStores {
    async fn enabled_pair_search_configs(&self) -> anyhow::Result<Vec<TokenPairSearchConfig>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl PoolStateStore for InMemoryStores {
    async fn set_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()> {
        self.pool_states
            .lock()
            .await
            .insert(pool_state.pool_id.address, pool_state);
        Ok(())
    }

    async fn set_pool_states(&self, pool_states: Vec<PoolState>) -> anyhow::Result<()> {
        let mut states = self.pool_states.lock().await;
        for pool_state in pool_states {
            states.insert(pool_state.pool_id.address, pool_state);
        }
        Ok(())
    }

    async fn get_pool_state(&self, address: Address) -> anyhow::Result<Option<PoolState>> {
        Ok(self.pool_states.lock().await.get(&address).cloned())
    }

    async fn get_pool_states(
        &self,
        addresses: &[Address],
    ) -> anyhow::Result<Vec<(Address, Option<PoolState>)>> {
        let pool_states = self.pool_states.lock().await;
        Ok(addresses
            .iter()
            .map(|address| (*address, pool_states.get(address).cloned()))
            .collect())
    }

    async fn all_pool_states(&self) -> anyhow::Result<Vec<PoolState>> {
        Ok(self.pool_states.lock().await.values().cloned().collect())
    }
}

#[async_trait]
impl PoolChangeStore for InMemoryStores {
    async fn mark_changed_pools(&self, pools: Vec<Address>) -> anyhow::Result<()> {
        let mut changed = self.changed_pools.lock().await;
        for pool in pools {
            changed.push_back(pool);
        }
        Ok(())
    }

    async fn drain_changed_pools(&self) -> anyhow::Result<Vec<Address>> {
        let mut changed = self.changed_pools.lock().await;
        let mut out = Vec::with_capacity(changed.len());
        while let Some(pool) = changed.pop_front() {
            out.push(pool);
        }
        Ok(out)
    }
}

#[async_trait]
impl TickChangeStore for InMemoryStores {
    async fn mark_tick_changed_pools(&self, pools: Vec<Address>) -> anyhow::Result<()> {
        let mut changed = self.tick_changed_pools.lock().await;
        for pool in pools {
            changed.push_back(pool);
        }
        Ok(())
    }

    async fn drain_tick_changed_pools(&self) -> anyhow::Result<Vec<Address>> {
        let mut changed = self.tick_changed_pools.lock().await;
        let mut out = Vec::with_capacity(changed.len());
        while let Some(pool) = changed.pop_front() {
            out.push(pool);
        }
        Ok(out)
    }
}

#[async_trait]
impl TickRepairStore for InMemoryStores {
    async fn mark_tick_repair_pools(&self, pools: Vec<Address>) -> anyhow::Result<()> {
        let mut changed = self.tick_changed_pools.lock().await;
        for pool in pools {
            changed.push_back(pool);
        }
        Ok(())
    }

    async fn drain_tick_repair_pools(&self, limit: usize) -> anyhow::Result<Vec<Address>> {
        let mut changed = self.tick_changed_pools.lock().await;
        let mut out = Vec::with_capacity(limit.min(changed.len()));
        while out.len() < limit {
            let Some(pool) = changed.pop_front() else {
                break;
            };
            out.push(pool);
        }
        Ok(out)
    }
}

#[async_trait]
impl CurrentBlockStore for InMemoryStores {
    async fn set_current_block(&self, block_number: u64) -> anyhow::Result<()> {
        let mut current = self.current_block.lock().await;
        *current = Some(current.map_or(block_number, |current| current.max(block_number)));
        Ok(())
    }

    async fn get_current_block(&self) -> anyhow::Result<Option<u64>> {
        Ok(*self.current_block.lock().await)
    }
}

impl PoolRuntimeStore for InMemoryStores {}

#[async_trait]
impl TickStateStore for InMemoryStores {
    async fn set_tick_state(&self, tick_state: TickState) -> anyhow::Result<()> {
        self.ticks
            .lock()
            .await
            .insert((tick_state.pool_id.address, tick_state.tick), tick_state);
        Ok(())
    }

    async fn set_tick_states(&self, tick_states: Vec<TickState>) -> anyhow::Result<()> {
        let mut ticks = self.ticks.lock().await;
        for tick_state in tick_states {
            ticks.insert((tick_state.pool_id.address, tick_state.tick), tick_state);
        }
        Ok(())
    }

    async fn replace_pool_ticks(
        &self,
        pool: Address,
        tick_states: Vec<TickState>,
    ) -> anyhow::Result<()> {
        let mut ticks = self.ticks.lock().await;
        ticks.retain(|(tick_pool, _), _| *tick_pool != pool);
        for tick_state in tick_states {
            ticks.insert((tick_state.pool_id.address, tick_state.tick), tick_state);
        }
        Ok(())
    }

    async fn get_pool_ticks(&self, pool: Address) -> anyhow::Result<Vec<TickState>> {
        Ok(self
            .ticks
            .lock()
            .await
            .values()
            .filter(|tick| tick.pool_id.address == pool)
            .cloned()
            .collect())
    }

    async fn get_pool_ticks_many(
        &self,
        pools: &[Address],
    ) -> anyhow::Result<HashMap<Address, Vec<TickState>>> {
        let wanted = pools
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let mut out: HashMap<Address, Vec<TickState>> = HashMap::new();
        for tick in self.ticks.lock().await.values() {
            if wanted.contains(&tick.pool_id.address) {
                out.entry(tick.pool_id.address)
                    .or_default()
                    .push(tick.clone());
            }
        }
        for ticks in out.values_mut() {
            ticks.sort_by_key(|tick| tick.tick);
        }
        Ok(out)
    }
}

#[async_trait]
impl CandidateStore for InMemoryStores {
    async fn push_candidate(&self, candidate: Candidate) -> anyhow::Result<()> {
        self.candidates.lock().await.push_back(candidate);
        Ok(())
    }

    async fn push_candidates(&self, candidates: &[Candidate]) -> anyhow::Result<()> {
        self.candidates
            .lock()
            .await
            .extend(candidates.iter().cloned());
        Ok(())
    }

    async fn prune_candidates_before_block(&self, block_number: u64) -> anyhow::Result<usize> {
        if block_number == 0 {
            return Ok(0);
        }
        let mut candidates = self.candidates.lock().await;
        let before = candidates.len();
        candidates.retain(|candidate| candidate.block_number >= block_number);
        Ok(before.saturating_sub(candidates.len()))
    }

    async fn pop_candidate(&self) -> anyhow::Result<Option<Candidate>> {
        Ok(self.candidates.lock().await.pop_front())
    }

    async fn pop_candidates(&self, limit: usize) -> anyhow::Result<Vec<Candidate>> {
        let mut candidates = self.candidates.lock().await;
        let mut out = Vec::with_capacity(limit.min(candidates.len()));
        while out.len() < limit {
            let Some(candidate) = candidates.pop_front() else {
                break;
            };
            out.push(candidate);
        }
        Ok(out)
    }
}

#[async_trait]
impl FailureStore for InMemoryStores {
    async fn mark_failure_key(&self, key: &str, _ttl_secs: u64) -> anyhow::Result<()> {
        self.failures
            .lock()
            .await
            .insert(key.to_string(), chrono::Utc::now());
        Ok(())
    }

    async fn has_failure_key(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.failures.lock().await.contains_key(key))
    }
}

#[async_trait]
impl RecorderStore for InMemoryStores {
    async fn record_dex_event(&self, event: DexEvent) -> anyhow::Result<()> {
        self.events.lock().await.push(event);
        Ok(())
    }

    async fn record_dex_events(&self, events: Vec<DexEvent>) -> anyhow::Result<()> {
        self.events.lock().await.extend(events);
        Ok(())
    }

    async fn record_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()> {
        self.pool_snapshots.lock().await.push(pool_state);
        Ok(())
    }

    async fn record_pool_states_with_source(
        &self,
        pool_states: Vec<PoolState>,
        _source: &str,
    ) -> anyhow::Result<()> {
        self.pool_snapshots.lock().await.extend(pool_states);
        Ok(())
    }

    async fn record_opportunity(&self, candidate: Candidate) -> anyhow::Result<()> {
        self.opportunities.lock().await.push(candidate);
        Ok(())
    }

    async fn record_opportunities(&self, candidates: Vec<Candidate>) -> anyhow::Result<()> {
        self.opportunities.lock().await.extend(candidates);
        Ok(())
    }

    async fn record_simulation(&self, simulation: SimulationResult) -> anyhow::Result<()> {
        self.simulations.lock().await.push(simulation);
        Ok(())
    }

    async fn record_simulations(&self, simulations: Vec<SimulationResult>) -> anyhow::Result<()> {
        self.simulations.lock().await.extend(simulations);
        Ok(())
    }

    async fn record_transaction(&self, tx: TxResult) -> anyhow::Result<()> {
        self.transactions.lock().await.push(tx);
        Ok(())
    }
}

#[async_trait]
impl EoaStateStore for InMemoryStores {
    async fn set_lane_state(&self, lane: EoaLaneState) -> anyhow::Result<()> {
        self.lanes.lock().await.insert(lane.address, lane);
        Ok(())
    }

    async fn get_lane_state(&self, address: Address) -> anyhow::Result<Option<EoaLaneState>> {
        Ok(self.lanes.lock().await.get(&address).cloned())
    }
}

#[async_trait]
impl PendingTransactionStore for InMemoryStores {
    async fn pending_transactions_for_eoa(
        &self,
        eoa: Address,
        limit: i64,
    ) -> anyhow::Result<Vec<PendingTransactionRecord>> {
        let limit = usize::try_from(limit.max(0)).unwrap_or_default();
        Ok(self
            .transactions
            .lock()
            .await
            .iter()
            .filter(|tx| {
                tx.eoa == eoa && matches!(tx.status, base_arb_common::types::TxStatus::Pending)
            })
            .filter_map(|tx| {
                Some(PendingTransactionRecord {
                    opportunity_id: tx.opportunity_id,
                    simulation_id: tx.simulation_id,
                    eoa: tx.eoa,
                    tx_hash: tx.tx_hash?,
                    nonce: tx.nonce,
                })
            })
            .take(limit)
            .collect())
    }

    async fn simulation_calldata(
        &self,
        simulation_id: uuid::Uuid,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .simulations
            .lock()
            .await
            .iter()
            .find(|simulation| simulation.id == simulation_id)
            .map(|simulation| simulation.calldata.clone()))
    }
}
