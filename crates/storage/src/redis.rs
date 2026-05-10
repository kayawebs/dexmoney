use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tracing::info;

use crate::{CandidateStore, EoaStateStore, PoolStateStore, TickStateStore};
use base_arb_common::types::{Candidate, EoaLaneState, PoolState, TickState};

#[derive(Clone)]
pub struct RedisStore {
    pub manager: ConnectionManager,
}

impl RedisStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let manager = ConnectionManager::new(client).await?;
        info!("connected to redis");
        Ok(Self { manager })
    }
}

pub fn pool_state_key(chain_id: u64, pool_address: alloy_primitives::Address) -> String {
    format!("pool:{chain_id}:{pool_address}")
}

pub fn tick_state_key(chain_id: u64, pool_address: alloy_primitives::Address, tick: i32) -> String {
    format!("ticks:{chain_id}:{pool_address}:{tick}")
}

pub fn candidates_key() -> &'static str {
    "candidates:priority"
}

pub fn eoa_lane_key(address: alloy_primitives::Address) -> String {
    format!("eoa:{address}:state")
}

pub fn failures_key(path_hash: &str) -> String {
    format!("failures:{path_hash}")
}

#[async_trait]
impl PoolStateStore for RedisStore {
    async fn set_pool_state(&self, pool_state: PoolState) -> Result<()> {
        let key = pool_state_key(pool_state.pool_id.chain_id, pool_state.pool_id.address);
        let value = serde_json::to_string(&pool_state)?;
        let mut manager = self.manager.clone();
        let _: () = manager.set(key, value).await?;
        Ok(())
    }

    async fn get_pool_state(&self, address: Address) -> Result<Option<PoolState>> {
        let mut manager = self.manager.clone();
        let pattern = format!("pool:*:{address}");
        let keys: Vec<String> = manager.keys(pattern).await?;
        let Some(key) = keys.into_iter().next() else {
            return Ok(None);
        };
        let value: Option<String> = manager.get(key).await?;
        value
            .map(|raw| serde_json::from_str(&raw))
            .transpose()
            .map_err(Into::into)
    }

    async fn all_pool_states(&self) -> Result<Vec<PoolState>> {
        let mut manager = self.manager.clone();
        let keys: Vec<String> = manager.keys("pool:*").await?;
        let mut out: Vec<PoolState> = Vec::with_capacity(keys.len());
        for key in keys {
            let value: Option<String> = manager.get(key).await?;
            if let Some(raw) = value {
                out.push(serde_json::from_str(&raw)?);
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl TickStateStore for RedisStore {
    async fn set_tick_state(&self, tick_state: TickState) -> Result<()> {
        let key = tick_state_key(
            tick_state.pool_id.chain_id,
            tick_state.pool_id.address,
            tick_state.tick,
        );
        let value = serde_json::to_string(&tick_state)?;
        let mut manager = self.manager.clone();
        let _: () = manager.set(key, value).await?;
        Ok(())
    }

    async fn set_tick_states(&self, tick_states: Vec<TickState>) -> Result<()> {
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        for tick_state in tick_states {
            let key = tick_state_key(
                tick_state.pool_id.chain_id,
                tick_state.pool_id.address,
                tick_state.tick,
            );
            let value = serde_json::to_string(&tick_state)?;
            pipe.set(key, value).ignore();
        }
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn get_pool_ticks(&self, pool: Address) -> Result<Vec<TickState>> {
        let mut manager = self.manager.clone();
        let pattern = format!("ticks:*:{pool}:*");
        let keys: Vec<String> = manager.keys(pattern).await?;
        let mut out: Vec<TickState> = Vec::with_capacity(keys.len());
        for key in keys {
            let value: Option<String> = manager.get(key).await?;
            if let Some(raw) = value {
                out.push(serde_json::from_str(&raw)?);
            }
        }
        out.sort_by_key(|tick| tick.tick);
        Ok(out)
    }
}

#[async_trait]
impl CandidateStore for RedisStore {
    async fn push_candidate(&self, candidate: Candidate) -> Result<()> {
        let mut manager = self.manager.clone();
        let score = candidate.expected_profit.to::<u128>() as f64;
        let member = serde_json::to_string(&candidate)?;
        let _: usize = manager.zadd(candidates_key(), member, score).await?;
        Ok(())
    }

    async fn pop_candidate(&self) -> Result<Option<Candidate>> {
        let mut manager = self.manager.clone();
        let result: Vec<(String, f64)> = redis::cmd("ZPOPMAX")
            .arg(candidates_key())
            .arg(1)
            .query_async(&mut manager)
            .await?;
        let Some((payload, _)) = result.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_str(&payload)?))
    }
}

#[async_trait]
impl EoaStateStore for RedisStore {
    async fn set_lane_state(&self, lane: EoaLaneState) -> Result<()> {
        let mut manager = self.manager.clone();
        let key = eoa_lane_key(lane.address);
        let value = serde_json::to_string(&lane)?;
        let _: () = manager.set(key, value).await?;
        Ok(())
    }

    async fn get_lane_state(&self, address: Address) -> Result<Option<EoaLaneState>> {
        let mut manager = self.manager.clone();
        let key = eoa_lane_key(address);
        let value: Option<String> = manager.get(key).await?;
        value
            .map(|raw| serde_json::from_str(&raw))
            .transpose()
            .map_err(Into::into)
    }
}
