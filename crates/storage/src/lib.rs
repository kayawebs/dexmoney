pub mod postgres;
pub mod redis;
pub mod schema;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use alloy_primitives::Address;
use async_trait::async_trait;
use tokio::sync::Mutex;

use base_arb_chain::events::DexEvent;
use base_arb_common::types::{Candidate, EoaLaneState, PoolState, SimulationResult, TxResult};

#[async_trait]
pub trait PoolStateStore: Send + Sync {
    async fn set_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()>;
    async fn get_pool_state(&self, address: Address) -> anyhow::Result<Option<PoolState>>;
    async fn all_pool_states(&self) -> anyhow::Result<Vec<PoolState>>;
}

#[async_trait]
pub trait CandidateStore: Send + Sync {
    async fn push_candidate(&self, candidate: Candidate) -> anyhow::Result<()>;
    async fn pop_candidate(&self) -> anyhow::Result<Option<Candidate>>;
}

#[async_trait]
pub trait RecorderStore: Send + Sync {
    async fn record_dex_event(&self, event: DexEvent) -> anyhow::Result<()>;
    async fn record_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()>;
    async fn record_pool_state_with_source(
        &self,
        pool_state: PoolState,
        source: &str,
    ) -> anyhow::Result<()> {
        let _ = source;
        self.record_pool_state(pool_state).await
    }
    async fn record_opportunity(&self, candidate: Candidate) -> anyhow::Result<()>;
    async fn record_simulation(&self, simulation: SimulationResult) -> anyhow::Result<()>;
    async fn record_transaction(&self, tx: TxResult) -> anyhow::Result<()>;
}

#[async_trait]
pub trait EoaStateStore: Send + Sync {
    async fn set_lane_state(&self, lane: EoaLaneState) -> anyhow::Result<()>;
    async fn get_lane_state(&self, address: Address) -> anyhow::Result<Option<EoaLaneState>>;
}

#[derive(Clone, Default)]
pub struct InMemoryStores {
    pool_states: Arc<Mutex<BTreeMap<Address, PoolState>>>,
    candidates: Arc<Mutex<VecDeque<Candidate>>>,
    opportunities: Arc<Mutex<Vec<Candidate>>>,
    simulations: Arc<Mutex<Vec<SimulationResult>>>,
    transactions: Arc<Mutex<Vec<TxResult>>>,
    lanes: Arc<Mutex<BTreeMap<Address, EoaLaneState>>>,
    events: Arc<Mutex<Vec<DexEvent>>>,
    pool_snapshots: Arc<Mutex<Vec<PoolState>>>,
}

impl InMemoryStores {
    pub fn new() -> Self {
        Self::default()
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

    async fn get_pool_state(&self, address: Address) -> anyhow::Result<Option<PoolState>> {
        Ok(self.pool_states.lock().await.get(&address).cloned())
    }

    async fn all_pool_states(&self) -> anyhow::Result<Vec<PoolState>> {
        Ok(self.pool_states.lock().await.values().cloned().collect())
    }
}

#[async_trait]
impl CandidateStore for InMemoryStores {
    async fn push_candidate(&self, candidate: Candidate) -> anyhow::Result<()> {
        self.candidates.lock().await.push_back(candidate);
        Ok(())
    }

    async fn pop_candidate(&self) -> anyhow::Result<Option<Candidate>> {
        Ok(self.candidates.lock().await.pop_front())
    }
}

#[async_trait]
impl RecorderStore for InMemoryStores {
    async fn record_dex_event(&self, event: DexEvent) -> anyhow::Result<()> {
        self.events.lock().await.push(event);
        Ok(())
    }

    async fn record_pool_state(&self, pool_state: PoolState) -> anyhow::Result<()> {
        self.pool_snapshots.lock().await.push(pool_state);
        Ok(())
    }

    async fn record_opportunity(&self, candidate: Candidate) -> anyhow::Result<()> {
        self.opportunities.lock().await.push(candidate);
        Ok(())
    }

    async fn record_simulation(&self, simulation: SimulationResult) -> anyhow::Result<()> {
        self.simulations.lock().await.push(simulation);
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
