use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::collections::HashMap;
use tracing::info;

use crate::{
    CandidateStore, CurrentBlockStore, EoaStateStore, FailureStore, PoolChangeStore,
    PoolStateStore, TickChangeStore, TickStateStore,
};
use base_arb_common::types::{Candidate, EoaLaneState, PoolState, TickState};

const REDIS_MGET_CHUNK_SIZE: usize = 1_000;

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

    pub async fn clear_candidates(&self) -> Result<usize> {
        let mut manager = self.manager.clone();
        let deleted: usize = manager.del(candidates_key()).await?;
        Ok(deleted)
    }
}

pub fn pool_state_key(chain_id: u64, pool_address: alloy_primitives::Address) -> String {
    format!("pool:{chain_id}:{pool_address}")
}

pub fn pool_address_index_key(pool_address: alloy_primitives::Address) -> String {
    format!("pool_index:{pool_address}")
}

pub fn tick_state_key(chain_id: u64, pool_address: alloy_primitives::Address, tick: i32) -> String {
    format!("ticks:{chain_id}:{pool_address}:{tick}")
}

pub fn tick_pool_index_key(pool_address: alloy_primitives::Address) -> String {
    format!("ticks:index:{pool_address}")
}

pub fn candidates_key() -> &'static str {
    "candidates:priority"
}

pub fn changed_pools_key() -> &'static str {
    "pools:changed"
}

pub fn changed_tick_pools_key() -> &'static str {
    "ticks:changed"
}

pub fn current_block_key() -> &'static str {
    "chain:current_block"
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
        let index_key = pool_address_index_key(pool_state.pool_id.address);
        let value = serde_json::to_string(&pool_state)?;
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        pipe.set(&key, value).ignore();
        pipe.set(index_key, key).ignore();
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn get_pool_state(&self, address: Address) -> Result<Option<PoolState>> {
        let mut manager = self.manager.clone();
        let index_key = pool_address_index_key(address);
        let mut key: Option<String> = manager.get(&index_key).await?;
        if key.is_none() {
            let pattern = format!("pool:*:{address}");
            let keys: Vec<String> = manager.keys(pattern).await?;
            key = keys.into_iter().next();
            if let Some(key) = key.as_ref() {
                let _: () = manager.set(&index_key, key).await?;
            }
        }
        let Some(key) = key else {
            return Ok(None);
        };
        let value: Option<String> = manager.get(key).await?;
        value
            .map(|raw| serde_json::from_str(&raw))
            .transpose()
            .map_err(Into::into)
    }

    async fn get_pool_states(
        &self,
        addresses: &[Address],
    ) -> Result<Vec<(Address, Option<PoolState>)>> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        let mut manager = self.manager.clone();
        let index_keys = addresses
            .iter()
            .map(|address| pool_address_index_key(*address))
            .collect::<Vec<_>>();
        let mut pool_keys: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&index_keys)
            .query_async(&mut manager)
            .await?;

        let mut missing_indexes = Vec::new();
        for (idx, key) in pool_keys.iter_mut().enumerate() {
            if key.is_some() {
                continue;
            }
            let address = addresses[idx];
            let pattern = format!("pool:*:{address}");
            let keys: Vec<String> = manager.keys(pattern).await?;
            if let Some(found) = keys.into_iter().next() {
                *key = Some(found.clone());
                missing_indexes.push((idx, found));
            }
        }
        if !missing_indexes.is_empty() {
            let mut pipe = redis::pipe();
            for (idx, key) in &missing_indexes {
                pipe.set(&index_keys[*idx], key).ignore();
            }
            let _: () = pipe.query_async(&mut manager).await?;
        }

        let existing_keys = pool_keys.iter().flatten().cloned().collect::<Vec<_>>();
        let values: Vec<Option<String>> = if existing_keys.is_empty() {
            Vec::new()
        } else {
            redis::cmd("MGET")
                .arg(&existing_keys)
                .query_async(&mut manager)
                .await?
        };
        let mut values_by_key = existing_keys
            .into_iter()
            .zip(values.into_iter())
            .collect::<std::collections::HashMap<_, _>>();

        addresses
            .iter()
            .zip(pool_keys.into_iter())
            .map(|(address, key)| {
                let state = key
                    .and_then(|key| values_by_key.remove(&key).flatten())
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()?;
                Ok((*address, state))
            })
            .collect()
    }

    async fn all_pool_states(&self) -> Result<Vec<PoolState>> {
        let mut manager = self.manager.clone();
        let keys: Vec<String> = manager.keys("pool:*").await?;
        let mut out: Vec<PoolState> = Vec::with_capacity(keys.len());
        for key_chunk in keys.chunks(REDIS_MGET_CHUNK_SIZE) {
            let values: Vec<Option<String>> = redis::cmd("MGET")
                .arg(key_chunk)
                .query_async(&mut manager)
                .await?;
            for value in values {
                if let Some(raw) = value {
                    out.push(serde_json::from_str(&raw)?);
                }
            }
        }
        if !out.is_empty() {
            let mut pipe = redis::pipe();
            for state in &out {
                pipe.set(
                    pool_address_index_key(state.pool_id.address),
                    pool_state_key(state.pool_id.chain_id, state.pool_id.address),
                )
                .ignore();
            }
            let _: () = pipe.query_async(&mut manager).await?;
        }
        Ok(out)
    }
}

#[async_trait]
impl PoolChangeStore for RedisStore {
    async fn mark_changed_pools(&self, pools: Vec<Address>) -> Result<()> {
        if pools.is_empty() {
            return Ok(());
        }
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        for pool in pools {
            pipe.sadd(changed_pools_key(), format!("{pool:#x}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn drain_changed_pools(&self) -> Result<Vec<Address>> {
        let mut manager = self.manager.clone();
        let values: Vec<String> = redis::cmd("SPOP")
            .arg(changed_pools_key())
            .arg(100_000usize)
            .query_async(&mut manager)
            .await?;
        if values.is_empty() {
            return Ok(Vec::new());
        }
        values
            .into_iter()
            .map(|value| value.parse().map_err(Into::into))
            .collect()
    }
}

#[async_trait]
impl TickChangeStore for RedisStore {
    async fn mark_tick_changed_pools(&self, pools: Vec<Address>) -> Result<()> {
        if pools.is_empty() {
            return Ok(());
        }
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        for pool in pools {
            pipe.sadd(changed_tick_pools_key(), format!("{pool:#x}"))
                .ignore();
        }
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn drain_tick_changed_pools(&self) -> Result<Vec<Address>> {
        let mut manager = self.manager.clone();
        let values: Vec<String> = redis::cmd("SPOP")
            .arg(changed_tick_pools_key())
            .arg(100_000usize)
            .query_async(&mut manager)
            .await?;
        if values.is_empty() {
            return Ok(Vec::new());
        }
        values
            .into_iter()
            .map(|value| value.parse().map_err(Into::into))
            .collect()
    }
}

#[async_trait]
impl CurrentBlockStore for RedisStore {
    async fn set_current_block(&self, block_number: u64) -> Result<()> {
        let mut manager = self.manager.clone();
        let current: Option<u64> = manager.get(current_block_key()).await?;
        if current.map_or(true, |current| block_number > current) {
            let _: () = manager.set(current_block_key(), block_number).await?;
        }
        Ok(())
    }

    async fn get_current_block(&self) -> Result<Option<u64>> {
        let mut manager = self.manager.clone();
        Ok(manager.get(current_block_key()).await?)
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
        let index_key = tick_pool_index_key(tick_state.pool_id.address);
        let value = serde_json::to_string(&tick_state)?;
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        pipe.set(&key, value).ignore();
        pipe.sadd(index_key, key).ignore();
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn set_tick_states(&self, tick_states: Vec<TickState>) -> Result<()> {
        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        for tick_state in tick_states {
            let pool = tick_state.pool_id.address;
            let key = tick_state_key(tick_state.pool_id.chain_id, pool, tick_state.tick);
            let index_key = tick_pool_index_key(pool);
            let value = serde_json::to_string(&tick_state)?;
            pipe.set(&key, value).ignore();
            pipe.sadd(index_key, key).ignore();
        }
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn replace_pool_ticks(&self, pool: Address, tick_states: Vec<TickState>) -> Result<()> {
        let mut manager = self.manager.clone();
        let index_key = tick_pool_index_key(pool);
        let mut existing_keys: Vec<String> = manager.smembers(&index_key).await?;
        if existing_keys.is_empty() {
            let pattern = format!("ticks:*:{pool}:*");
            existing_keys = manager.keys(pattern).await?;
        }
        let mut pipe = redis::pipe();
        if !existing_keys.is_empty() {
            pipe.del(existing_keys).ignore();
        }
        pipe.del(&index_key).ignore();
        for tick_state in tick_states {
            let key = tick_state_key(
                tick_state.pool_id.chain_id,
                tick_state.pool_id.address,
                tick_state.tick,
            );
            let value = serde_json::to_string(&tick_state)?;
            pipe.set(&key, value).ignore();
            pipe.sadd(&index_key, key).ignore();
        }
        let _: () = pipe.query_async(&mut manager).await?;
        Ok(())
    }

    async fn get_pool_ticks(&self, pool: Address) -> Result<Vec<TickState>> {
        let mut manager = self.manager.clone();
        let index_key = tick_pool_index_key(pool);
        let mut keys: Vec<String> = manager.smembers(&index_key).await?;
        if keys.is_empty() {
            let pattern = format!("ticks:*:{pool}:*");
            keys = manager.keys(pattern).await?;
            if !keys.is_empty() {
                let mut pipe = redis::pipe();
                for key in &keys {
                    pipe.sadd(&index_key, key).ignore();
                }
                let _: () = pipe.query_async(&mut manager).await?;
            }
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut out: Vec<TickState> = Vec::with_capacity(keys.len());
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut manager)
            .await?;
        for value in values {
            if let Some(raw) = value {
                out.push(serde_json::from_str(&raw)?);
            }
        }
        out.sort_by_key(|tick| tick.tick);
        Ok(out)
    }

    async fn get_pool_ticks_many(
        &self,
        pools: &[Address],
    ) -> Result<HashMap<Address, Vec<TickState>>> {
        let mut out = pools
            .iter()
            .copied()
            .map(|pool| (pool, Vec::new()))
            .collect::<HashMap<_, _>>();
        if pools.is_empty() {
            return Ok(out);
        }

        let mut unique_pools = pools.to_vec();
        unique_pools.sort_unstable();
        unique_pools.dedup();

        let mut manager = self.manager.clone();
        let mut pipe = redis::pipe();
        for pool in &unique_pools {
            pipe.smembers(tick_pool_index_key(*pool));
        }
        let key_sets: Vec<Vec<String>> = pipe.query_async(&mut manager).await?;

        let mut all_keys = Vec::new();
        let mut key_to_pool = HashMap::new();
        for (pool, keys) in unique_pools.iter().zip(key_sets) {
            for key in keys {
                key_to_pool.insert(key.clone(), *pool);
                all_keys.push(key);
            }
        }

        if all_keys.is_empty() {
            return Ok(out);
        }

        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&all_keys)
            .query_async(&mut manager)
            .await?;
        for (key, value) in all_keys.into_iter().zip(values) {
            let Some(raw) = value else {
                continue;
            };
            let tick: TickState = serde_json::from_str(&raw)?;
            let pool = key_to_pool
                .get(&key)
                .copied()
                .unwrap_or(tick.pool_id.address);
            out.entry(pool).or_default().push(tick);
        }
        for ticks in out.values_mut() {
            ticks.sort_by_key(|tick| tick.tick);
        }
        Ok(out)
    }
}

#[async_trait]
impl CandidateStore for RedisStore {
    async fn push_candidate(&self, candidate: Candidate) -> Result<()> {
        let mut manager = self.manager.clone();
        let score = candidate_queue_score(&candidate);
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

    async fn pop_candidates(&self, limit: usize) -> Result<Vec<Candidate>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut manager = self.manager.clone();
        let result: Vec<(String, f64)> = redis::cmd("ZPOPMAX")
            .arg(candidates_key())
            .arg(limit)
            .query_async(&mut manager)
            .await?;
        result
            .into_iter()
            .map(|(payload, _)| serde_json::from_str(&payload).map_err(Into::into))
            .collect()
    }
}

fn candidate_queue_score(candidate: &Candidate) -> f64 {
    let created_rank = candidate.created_at.timestamp_millis().rem_euclid(100_000) as u64;
    candidate.block_number.saturating_mul(100_000) as f64 + created_rank as f64
}

#[async_trait]
impl FailureStore for RedisStore {
    async fn mark_failure_key(&self, key: &str, ttl_secs: u64) -> Result<()> {
        let mut manager = self.manager.clone();
        let redis_key = failures_key(key);
        let _: () = manager.set_ex(redis_key, "1", ttl_secs).await?;
        Ok(())
    }

    async fn has_failure_key(&self, key: &str) -> Result<bool> {
        let mut manager = self.manager.clone();
        let redis_key = failures_key(key);
        Ok(manager.exists(redis_key).await?)
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
