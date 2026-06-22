use alloy_primitives::{Address, B256, U256};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{PgPool, Postgres, QueryBuilder};
use tracing::info;

use crate::{
    PairSearchConfigStore, PendingTransactionRecord, PendingTransactionStore, RecorderStore,
};
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{
    Candidate, DexKind, DiscoveredPool, PoolRegistryEntry, PoolState, PoolStateValidation,
    PoolStateWarning, PoolVariant, SimulationResult, TickState, TokenPairSearchConfig, TxResult,
    V3LiquidityUpdate,
};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct FactoryRegistryRecord {
    pub chain_id: i64,
    pub factory_address: String,
    pub dex: String,
    pub variant: String,
    pub trusted: bool,
    pub enabled: bool,
    pub source: String,
    pub notes: Option<String>,
    pub observed_pools: i64,
    pub first_seen_block: Option<i64>,
    pub latest_seen_block: Option<i64>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct ProtocolPoolObservation {
    pub chain_id: u64,
    pub protocol: String,
    pub manager_address: Address,
    pub pool_uid: String,
    pub pool_address: Option<Address>,
    pub topic0: String,
    pub event_type: String,
    pub token0: Option<Address>,
    pub token1: Option<Address>,
    pub symbol: Option<String>,
    pub factory_address: Option<Address>,
    pub dex: Option<String>,
    pub variant: Option<String>,
    pub fee_bps: Option<u32>,
    pub fee_pips: Option<u32>,
    pub pool_key_fee_pips: Option<u32>,
    pub tick_spacing: Option<i32>,
    pub hooks_address: Option<Address>,
    pub sqrt_price_x96: Option<U256>,
    pub liquidity: Option<U256>,
    pub tick: Option<i32>,
    pub block_number: u64,
    pub discovery_source: String,
    pub import_status: String,
    pub import_reason: Option<String>,
    pub raw_json: serde_json::Value,
}

#[derive(Clone)]
pub struct PostgresStore {
    pub pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        ensure_registry_schema(&pool).await?;
        info!("connected to postgres");
        Ok(Self { pool })
    }

    pub async fn upsert_token_pair(
        &self,
        chain_id: u64,
        token0: Address,
        token1: Address,
        symbol: &str,
    ) -> Result<uuid::Uuid> {
        let id: uuid::Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO token_pairs (id, chain_id, token0, token1, symbol, enabled, created_at, updated_at)
            VALUES (uuid_generate_v4(), $1, $2, $3, $4, TRUE, NOW(), NOW())
            ON CONFLICT (chain_id, token0, token1)
            DO UPDATE SET symbol = EXCLUDED.symbol, enabled = TRUE, updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(token0))
        .bind(address_to_string(token1))
        .bind(symbol)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn upsert_token_registry(
        &self,
        chain_id: u64,
        token: Address,
        symbol: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO tokens (chain_id, token_address, symbol, enabled, created_at, updated_at)
            VALUES ($1, $2, $3, FALSE, NOW(), NOW())
            ON CONFLICT (chain_id, token_address)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                updated_at = NOW()
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(token))
        .bind(symbol)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_discovered_pool(
        &self,
        token_pair_id: uuid::Uuid,
        discovered: &DiscoveredPool,
    ) -> Result<()> {
        let state = &discovered.state;
        sqlx::query(
            r#"
            INSERT INTO pools (
                id, token_pair_id, chain_id, pool_address, dex, variant, token0, token1,
                fee_bps, tick_spacing, stable, factory_address, enabled, source, created_at, updated_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,TRUE,$12,NOW(),NOW())
            ON CONFLICT (chain_id, pool_address)
            DO UPDATE SET
                token_pair_id = EXCLUDED.token_pair_id,
                dex = EXCLUDED.dex,
                variant = EXCLUDED.variant,
                token0 = EXCLUDED.token0,
                token1 = EXCLUDED.token1,
                fee_bps = EXCLUDED.fee_bps,
                tick_spacing = EXCLUDED.tick_spacing,
                stable = EXCLUDED.stable,
                factory_address = EXCLUDED.factory_address,
                enabled = TRUE,
                source = EXCLUDED.source,
                updated_at = NOW()
            "#,
        )
        .bind(token_pair_id)
        .bind(i64::try_from(state.pool_id.chain_id)?)
        .bind(address_to_string(state.pool_id.address))
        .bind(dex_to_string(state.dex))
        .bind(variant_to_string(state.variant))
        .bind(address_to_string(state.token0))
        .bind(address_to_string(state.token1))
        .bind(i64::from(state.fee_bps))
        .bind(discovered.tick_spacing.map(i64::from))
        .bind(discovered.stable)
        .bind(discovered.factory_address.map(address_to_string))
        .bind(&discovered.source)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_factory_registry(
        &self,
        chain_id: u64,
        factory_address: Address,
        dex: &str,
        variant: &str,
        trusted: bool,
        enabled: bool,
        source: &str,
        notes: Option<&str>,
        first_seen_block: Option<i64>,
        latest_seen_block: Option<i64>,
        observed_pools_delta: i64,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO factory_registry (
                chain_id, factory_address, dex, variant, trusted, enabled, source, notes,
                first_seen_block, latest_seen_block, observed_pools, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,GREATEST($11, 0),NOW(),NOW())
            ON CONFLICT (chain_id, factory_address)
            DO UPDATE SET
                dex = EXCLUDED.dex,
                variant = EXCLUDED.variant,
                trusted = EXCLUDED.trusted,
                enabled = EXCLUDED.enabled,
                source = EXCLUDED.source,
                notes = COALESCE(EXCLUDED.notes, factory_registry.notes),
                first_seen_block = COALESCE(
                    LEAST(factory_registry.first_seen_block, EXCLUDED.first_seen_block),
                    factory_registry.first_seen_block,
                    EXCLUDED.first_seen_block
                ),
                latest_seen_block = GREATEST(
                    COALESCE(factory_registry.latest_seen_block, 0),
                    COALESCE(EXCLUDED.latest_seen_block, 0)
                ),
                observed_pools = factory_registry.observed_pools + GREATEST($11, 0),
                updated_at = NOW()
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(factory_address))
        .bind(dex)
        .bind(variant)
        .bind(trusted)
        .bind(enabled)
        .bind(source)
        .bind(notes)
        .bind(first_seen_block)
        .bind(latest_seen_block)
        .bind(observed_pools_delta)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_protocol_pool_observation(
        &self,
        observation: ProtocolPoolObservation,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO protocol_pool_observations (
                chain_id, protocol, manager_address, pool_uid, pool_address, topic0, event_type,
                token0, token1, symbol, factory_address, dex, variant, fee_bps, fee_pips,
                pool_key_fee_pips, tick_spacing, hooks_address, sqrt_price_x96, liquidity, tick,
                first_block, latest_block, logs_30d, discovery_source, import_status,
                import_reason, raw_json, created_at, updated_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                $21,$22,$22,1,$23,$24,$25,$26,NOW(),NOW()
            )
            ON CONFLICT (chain_id, protocol, manager_address, pool_uid)
            DO UPDATE SET
                pool_address = COALESCE(EXCLUDED.pool_address, protocol_pool_observations.pool_address),
                topic0 = EXCLUDED.topic0,
                event_type = EXCLUDED.event_type,
                token0 = COALESCE(EXCLUDED.token0, protocol_pool_observations.token0),
                token1 = COALESCE(EXCLUDED.token1, protocol_pool_observations.token1),
                symbol = COALESCE(EXCLUDED.symbol, protocol_pool_observations.symbol),
                factory_address = COALESCE(EXCLUDED.factory_address, protocol_pool_observations.factory_address),
                dex = COALESCE(EXCLUDED.dex, protocol_pool_observations.dex),
                variant = COALESCE(EXCLUDED.variant, protocol_pool_observations.variant),
                fee_bps = COALESCE(EXCLUDED.fee_bps, protocol_pool_observations.fee_bps),
                fee_pips = COALESCE(EXCLUDED.fee_pips, protocol_pool_observations.fee_pips),
                pool_key_fee_pips = COALESCE(EXCLUDED.pool_key_fee_pips, protocol_pool_observations.pool_key_fee_pips),
                tick_spacing = COALESCE(EXCLUDED.tick_spacing, protocol_pool_observations.tick_spacing),
                hooks_address = COALESCE(EXCLUDED.hooks_address, protocol_pool_observations.hooks_address),
                sqrt_price_x96 = COALESCE(EXCLUDED.sqrt_price_x96, protocol_pool_observations.sqrt_price_x96),
                liquidity = COALESCE(EXCLUDED.liquidity, protocol_pool_observations.liquidity),
                tick = COALESCE(EXCLUDED.tick, protocol_pool_observations.tick),
                first_block = LEAST(protocol_pool_observations.first_block, EXCLUDED.first_block),
                latest_block = GREATEST(protocol_pool_observations.latest_block, EXCLUDED.latest_block),
                logs_30d = protocol_pool_observations.logs_30d + 1,
                discovery_source = EXCLUDED.discovery_source,
                import_status = EXCLUDED.import_status,
                import_reason = EXCLUDED.import_reason,
                raw_json = EXCLUDED.raw_json,
                updated_at = NOW()
            "#,
        )
        .bind(i64::try_from(observation.chain_id)?)
        .bind(observation.protocol)
        .bind(address_to_string(observation.manager_address))
        .bind(observation.pool_uid)
        .bind(observation.pool_address.map(address_to_string))
        .bind(observation.topic0)
        .bind(observation.event_type)
        .bind(observation.token0.map(address_to_string))
        .bind(observation.token1.map(address_to_string))
        .bind(observation.symbol)
        .bind(observation.factory_address.map(address_to_string))
        .bind(observation.dex)
        .bind(observation.variant)
        .bind(observation.fee_bps.map(i64::from))
        .bind(observation.fee_pips.map(i64::from))
        .bind(observation.pool_key_fee_pips.map(i64::from))
        .bind(observation.tick_spacing.map(i64::from))
        .bind(observation.hooks_address.map(address_to_string))
        .bind(observation.sqrt_price_x96.map(|value| value.to_string()))
        .bind(observation.liquidity.map(|value| value.to_string()))
        .bind(observation.tick.map(i64::from))
        .bind(i64::try_from(observation.block_number)?)
        .bind(observation.discovery_source)
        .bind(observation.import_status)
        .bind(observation.import_reason)
        .bind(sqlx::types::Json(observation.raw_json))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn replace_pool_ticks_current(
        &self,
        chain_id: u64,
        pool_address: Address,
        tick_states: &[TickState],
        source: &str,
    ) -> Result<()> {
        let chain_id = i64::try_from(chain_id)?;
        let pool_address = address_to_string(pool_address);
        let Some(first) = tick_states.first() else {
            sqlx::query(
                r#"
                DELETE FROM pool_ticks_current
                WHERE chain_id = $1
                  AND lower(pool_address) = lower($2)
                "#,
            )
            .bind(chain_id)
            .bind(pool_address)
            .execute(&self.pool)
            .await?;
            return Ok(());
        };

        debug_assert_eq!(i64::try_from(first.pool_id.chain_id).ok(), Some(chain_id));
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            DELETE FROM pool_ticks_current
            WHERE chain_id = $1
              AND lower(pool_address) = lower($2)
            "#,
        )
        .bind(chain_id)
        .bind(&pool_address)
        .execute(&mut *tx)
        .await?;

        let chain_ids = vec![chain_id; tick_states.len()];
        let pool_addresses = vec![pool_address.clone(); tick_states.len()];
        let ticks = tick_states.iter().map(|tick| tick.tick).collect::<Vec<_>>();
        let liquidity_nets = tick_states
            .iter()
            .map(|tick| tick.liquidity_net.to_string())
            .collect::<Vec<_>>();
        let liquidity_grosses = tick_states
            .iter()
            .map(|tick| tick.liquidity_gross.to_string())
            .collect::<Vec<_>>();
        let block_numbers = tick_states
            .iter()
            .map(|tick| i64::try_from(tick.block_number))
            .collect::<Result<Vec<_>, _>>()?;
        let sources = vec![source.to_string(); tick_states.len()];
        let updated_ats = tick_states
            .iter()
            .map(|tick| tick.updated_at)
            .collect::<Vec<_>>();
        sqlx::query(
            r#"
            INSERT INTO pool_ticks_current (
                chain_id, pool_address, tick, liquidity_net, liquidity_gross,
                block_number, source, updated_at
            )
            SELECT *
            FROM UNNEST(
                $1::BIGINT[],
                $2::TEXT[],
                $3::INTEGER[],
                $4::TEXT[],
                $5::TEXT[],
                $6::BIGINT[],
                $7::TEXT[],
                $8::TIMESTAMPTZ[]
            )
            "#,
        )
        .bind(chain_ids)
        .bind(pool_addresses)
        .bind(ticks)
        .bind(liquidity_nets)
        .bind(liquidity_grosses)
        .bind(block_numbers)
        .bind(sources)
        .bind(updated_ats)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn start_pool_tick_hydration_run(
        &self,
        chain_id: u64,
        protocol: &str,
        manager_address: Option<Address>,
        from_block: u64,
        to_block: u64,
        selected_pools: usize,
    ) -> Result<uuid::Uuid> {
        let id: uuid::Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO pool_tick_hydration_runs (
                chain_id, protocol, manager_address, from_block, to_block,
                selected_pools, hydrated_pools, ticks_written, status,
                started_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 'running', NOW())
            RETURNING id
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(protocol)
        .bind(manager_address.map(address_to_string))
        .bind(i64::try_from(from_block)?)
        .bind(i64::try_from(to_block)?)
        .bind(i64::try_from(selected_pools)?)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn finish_pool_tick_hydration_run(
        &self,
        id: uuid::Uuid,
        hydrated_pools: usize,
        ticks_written: usize,
        status: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pool_tick_hydration_runs
            SET hydrated_pools = $2,
                ticks_written = $3,
                status = $4,
                finished_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(i64::try_from(hydrated_pools)?)
        .bind(i64::try_from(ticks_written)?)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_pool_tick_hydration_run_progress(
        &self,
        id: uuid::Uuid,
        hydrated_pools: usize,
        ticks_written: usize,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE pool_tick_hydration_runs
            SET hydrated_pools = $2,
                ticks_written = $3
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(i64::try_from(hydrated_pools)?)
        .bind(i64::try_from(ticks_written)?)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn trusted_factory_registry(
        &self,
        chain_id: u64,
    ) -> Result<Vec<FactoryRegistryRecord>> {
        Ok(sqlx::query_as::<_, FactoryRegistryRecord>(
            r#"
            SELECT
                chain_id, factory_address, dex, variant, trusted, enabled, source, notes,
                observed_pools, first_seen_block, latest_seen_block, updated_at
            FROM factory_registry
            WHERE chain_id = $1
              AND trusted = TRUE
              AND enabled = TRUE
            ORDER BY updated_at DESC
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn all_factory_registry(
        &self,
        chain_id: u64,
        limit: i64,
    ) -> Result<Vec<FactoryRegistryRecord>> {
        Ok(sqlx::query_as::<_, FactoryRegistryRecord>(
            r#"
            SELECT
                chain_id, factory_address, dex, variant, trusted, enabled, source, notes,
                observed_pools, first_seen_block, latest_seen_block, updated_at
            FROM factory_registry
            WHERE chain_id = $1
            ORDER BY trusted DESC, observed_pools DESC, updated_at DESC
            LIMIT $2
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_observed_pool(
        &self,
        chain_id: u64,
        pool_address: Address,
        topic0: &str,
        family: &str,
        token0: Option<Address>,
        token1: Option<Address>,
        symbol: Option<&str>,
        factory_address: Option<Address>,
        dex: Option<&str>,
        variant: Option<&str>,
        fee_bps: Option<u32>,
        fee_pips: Option<u32>,
        tick_spacing: Option<i32>,
        stable: Option<bool>,
        txs_30d: i64,
        logs_30d: i64,
        first_block: Option<i64>,
        latest_block: Option<i64>,
        discovery_source: &str,
        import_status: &str,
        import_reason: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO observed_pools (
                chain_id, pool_address, topic0, family, token0, token1, symbol,
                factory_address, dex, variant, fee_bps, fee_pips, tick_spacing, stable,
                txs_30d, logs_30d, first_block, latest_block, discovery_source,
                import_status, import_reason,
                created_at, updated_at
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,NOW(),NOW()
            )
            ON CONFLICT (chain_id, pool_address)
            DO UPDATE SET
                topic0 = EXCLUDED.topic0,
                family = EXCLUDED.family,
                token0 = EXCLUDED.token0,
                token1 = EXCLUDED.token1,
                symbol = EXCLUDED.symbol,
                factory_address = EXCLUDED.factory_address,
                dex = EXCLUDED.dex,
                variant = EXCLUDED.variant,
                fee_bps = EXCLUDED.fee_bps,
                fee_pips = EXCLUDED.fee_pips,
                tick_spacing = EXCLUDED.tick_spacing,
                stable = EXCLUDED.stable,
                txs_30d = EXCLUDED.txs_30d,
                logs_30d = EXCLUDED.logs_30d,
                first_block = EXCLUDED.first_block,
                latest_block = EXCLUDED.latest_block,
                discovery_source = EXCLUDED.discovery_source,
                import_status = EXCLUDED.import_status,
                import_reason = EXCLUDED.import_reason,
                updated_at = NOW()
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(pool_address))
        .bind(topic0.to_ascii_lowercase())
        .bind(family)
        .bind(token0.map(address_to_string))
        .bind(token1.map(address_to_string))
        .bind(symbol)
        .bind(factory_address.map(address_to_string))
        .bind(dex)
        .bind(variant)
        .bind(fee_bps.map(i64::from))
        .bind(fee_pips.map(i64::from))
        .bind(tick_spacing.map(i64::from))
        .bind(stable)
        .bind(txs_30d)
        .bind(logs_30d)
        .bind(first_block)
        .bind(latest_block)
        .bind(discovery_source)
        .bind(import_status)
        .bind(import_reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_token_pair_search_config(
        &self,
        chain_id: u64,
        token0: Address,
        token1: Address,
        token0_search_amounts: Option<&str>,
        token1_search_amounts: Option<&str>,
        token0_min_profit: Option<&str>,
        token1_min_profit: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE token_pairs
            SET
                token0_search_amounts = $4,
                token1_search_amounts = $5,
                token0_min_profit = $6,
                token1_min_profit = $7,
                updated_at = NOW()
            WHERE chain_id = $1 AND token0 = $2 AND token1 = $3
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(token0))
        .bind(address_to_string(token1))
        .bind(token0_search_amounts)
        .bind(token1_search_amounts)
        .bind(token0_min_profit)
        .bind(token1_min_profit)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_token_search_default(
        &self,
        chain_id: u64,
        token_address: Address,
        executor_scope: &str,
        search_amounts: Option<&str>,
        min_profit: Option<&str>,
    ) -> Result<()> {
        let chain_id = i64::try_from(chain_id)?;
        let token_address = address_to_string(token_address);
        let executor_scope = normalize_executor_scope(executor_scope)?;
        match (search_amounts, min_profit) {
            (Some(search_amounts), Some(min_profit)) => {
                sqlx::query(
                    r#"
                    INSERT INTO token_search_defaults (
                        chain_id, token_address, executor_scope, search_amounts, min_profit, created_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,NOW(),NOW())
                    ON CONFLICT (chain_id, token_address, executor_scope)
                    DO UPDATE SET
                        search_amounts = EXCLUDED.search_amounts,
                        min_profit = EXCLUDED.min_profit,
                        updated_at = NOW()
                    "#,
                )
                .bind(chain_id)
                .bind(token_address)
                .bind(executor_scope)
                .bind(search_amounts)
                .bind(min_profit)
                .execute(&self.pool)
                .await?;
            }
            (None, None) => {
                sqlx::query(
                    r#"
                    DELETE FROM token_search_defaults
                    WHERE chain_id = $1 AND token_address = $2 AND executor_scope = $3
                    "#,
                )
                .bind(chain_id)
                .bind(token_address)
                .bind(executor_scope)
                .execute(&self.pool)
                .await?;
            }
            _ => anyhow::bail!("token default amounts and min profit must be set together"),
        }
        Ok(())
    }

    pub async fn disable_token_pair(
        &self,
        chain_id: u64,
        token0: Address,
        token1: Address,
    ) -> Result<()> {
        let token_pair_id: Option<uuid::Uuid> = sqlx::query_scalar(
            r#"
            UPDATE token_pairs
            SET enabled = FALSE, updated_at = NOW()
            WHERE chain_id = $1 AND token0 = $2 AND token1 = $3
            RETURNING id
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(token0))
        .bind(address_to_string(token1))
        .fetch_optional(&self.pool)
        .await?;

        if let Some(token_pair_id) = token_pair_id {
            sqlx::query(
                r#"
                UPDATE pools
                SET enabled = FALSE, updated_at = NOW()
                WHERE token_pair_id = $1
                "#,
            )
            .bind(token_pair_id)
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub async fn delete_token_pair(
        &self,
        chain_id: u64,
        token0: Address,
        token1: Address,
    ) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let token_pair_id: Option<uuid::Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM token_pairs
            WHERE chain_id = $1 AND token0 = $2 AND token1 = $3
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(address_to_string(token0))
        .bind(address_to_string(token1))
        .fetch_optional(&mut *tx)
        .await?;

        let Some(token_pair_id) = token_pair_id else {
            tx.commit().await?;
            return Ok(0);
        };

        let pools_deleted = sqlx::query(
            r#"
            DELETE FROM pools
            WHERE token_pair_id = $1
            "#,
        )
        .bind(token_pair_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        sqlx::query(
            r#"
            DELETE FROM token_pairs
            WHERE id = $1
            "#,
        )
        .bind(token_pair_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(pools_deleted)
    }

    pub async fn enabled_registry_pools(&self) -> Result<Vec<PoolRegistryEntry>> {
        let rows = sqlx::query_as::<_, PoolRegistryRow>(
            r#"
            SELECT DISTINCT ON (lower(pool_address))
                pool_address, dex, variant, factory_address, token0, token1, fee_bps,
                tick_spacing, stable, enabled
            FROM pools
            WHERE enabled = TRUE
            ORDER BY lower(pool_address), updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(PoolRegistryEntry::try_from).collect()
    }

    pub async fn enabled_pair_search_configs(&self) -> Result<Vec<TokenPairSearchConfig>> {
        let rows = sqlx::query_as::<_, TokenPairSearchConfigRow>(
            r#"
            SELECT
                tp.chain_id,
                tp.token0,
                tp.token1,
                tp.symbol,
                CASE
                    WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                        THEN tp.token0_search_amounts
                    ELSE COALESCE(token0_two_hop_default.search_amounts, token0_default.search_amounts)
                END AS token0_search_amounts,
                CASE
                    WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                        THEN tp.token1_search_amounts
                    ELSE COALESCE(token1_two_hop_default.search_amounts, token1_default.search_amounts)
                END AS token1_search_amounts,
                CASE
                    WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                        THEN tp.token0_search_amounts
                    ELSE COALESCE(token0_multihop_default.search_amounts, token0_default.search_amounts)
                END AS token0_multihop_search_amounts,
                CASE
                    WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                        THEN tp.token1_search_amounts
                    ELSE COALESCE(token1_multihop_default.search_amounts, token1_default.search_amounts)
                END AS token1_multihop_search_amounts,
                CASE
                    WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                        THEN tp.token0_min_profit
                    ELSE COALESCE(token0_two_hop_default.min_profit, token0_default.min_profit)
                END AS token0_min_profit,
                CASE
                    WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                        THEN tp.token1_min_profit
                    ELSE COALESCE(token1_two_hop_default.min_profit, token1_default.min_profit)
                END AS token1_min_profit,
                CASE
                    WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                        THEN tp.token0_min_profit
                    ELSE COALESCE(token0_multihop_default.min_profit, token0_default.min_profit)
                END AS token0_multihop_min_profit,
                CASE
                    WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                        THEN tp.token1_min_profit
                    ELSE COALESCE(token1_multihop_default.min_profit, token1_default.min_profit)
                END AS token1_multihop_min_profit,
                token0_default.min_profit AS token0_all_min_profit,
                token1_default.min_profit AS token1_all_min_profit
            FROM token_pairs tp
            LEFT JOIN token_search_defaults token0_default
              ON token0_default.chain_id = tp.chain_id
             AND token0_default.token_address = tp.token0
             AND token0_default.executor_scope = 'all'
            LEFT JOIN token_search_defaults token0_two_hop_default
              ON token0_two_hop_default.chain_id = tp.chain_id
             AND token0_two_hop_default.token_address = tp.token0
             AND token0_two_hop_default.executor_scope = 'two_hop'
            LEFT JOIN token_search_defaults token0_multihop_default
              ON token0_multihop_default.chain_id = tp.chain_id
             AND token0_multihop_default.token_address = tp.token0
             AND token0_multihop_default.executor_scope = 'multihop'
            LEFT JOIN token_search_defaults token1_default
              ON token1_default.chain_id = tp.chain_id
             AND token1_default.token_address = tp.token1
             AND token1_default.executor_scope = 'all'
            LEFT JOIN token_search_defaults token1_two_hop_default
              ON token1_two_hop_default.chain_id = tp.chain_id
             AND token1_two_hop_default.token_address = tp.token1
             AND token1_two_hop_default.executor_scope = 'two_hop'
            LEFT JOIN token_search_defaults token1_multihop_default
              ON token1_multihop_default.chain_id = tp.chain_id
             AND token1_multihop_default.token_address = tp.token1
             AND token1_multihop_default.executor_scope = 'multihop'
            WHERE tp.enabled = TRUE
              AND (
                COALESCE(
                    NULLIF(BTRIM(tp.token0_search_amounts), ''),
                    NULLIF(BTRIM(token0_two_hop_default.search_amounts), ''),
                    NULLIF(BTRIM(token0_multihop_default.search_amounts), ''),
                    NULLIF(BTRIM(token0_default.search_amounts), '')
                ) IS NOT NULL
                OR COALESCE(
                    NULLIF(BTRIM(tp.token1_search_amounts), ''),
                    NULLIF(BTRIM(token1_two_hop_default.search_amounts), ''),
                    NULLIF(BTRIM(token1_multihop_default.search_amounts), ''),
                    NULLIF(BTRIM(token1_default.search_amounts), '')
                ) IS NOT NULL
              )
            ORDER BY tp.updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .filter_map(|row| match TokenPairSearchConfig::try_from(row) {
                Ok(config)
                    if !config.token0_search_amounts.is_empty()
                        || !config.token1_search_amounts.is_empty() =>
                {
                    Some(Ok(config))
                }
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .collect()
    }

    pub async fn record_pool_state_warning(&self, warning: PoolStateWarning) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO pool_state_warnings (
                id, pool_address, dex, variant, block_number, local_state_json,
                onchain_state_json, drift_bps, message, created_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9)
            "#,
        )
        .bind(address_to_string(warning.pool_address))
        .bind(dex_to_string(warning.dex))
        .bind(variant_to_string(warning.variant))
        .bind(i64::try_from(warning.block_number)?)
        .bind(sqlx::types::Json(warning.local_state))
        .bind(sqlx::types::Json(warning.onchain_state))
        .bind(i64::try_from(warning.drift_bps)?)
        .bind(warning.message)
        .bind(warning.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_pool_state_validation(
        &self,
        validation: PoolStateValidation,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO pool_state_validations (
                id, pool_address, dex, variant, block_number, block_hash,
                local_state_json, onchain_state_json, drift_bps, passed, message, created_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (pool_address, block_number)
            DO UPDATE SET
                block_hash = EXCLUDED.block_hash,
                local_state_json = EXCLUDED.local_state_json,
                onchain_state_json = EXCLUDED.onchain_state_json,
                drift_bps = EXCLUDED.drift_bps,
                passed = EXCLUDED.passed,
                message = EXCLUDED.message,
                created_at = EXCLUDED.created_at
            "#,
        )
        .bind(address_to_string(validation.pool_address))
        .bind(dex_to_string(validation.dex))
        .bind(variant_to_string(validation.variant))
        .bind(i64::try_from(validation.block_number)?)
        .bind(validation.block_hash)
        .bind(sqlx::types::Json(validation.local_state))
        .bind(sqlx::types::Json(validation.onchain_state))
        .bind(i64::try_from(validation.drift_bps)?)
        .bind(validation.passed)
        .bind(validation.message)
        .bind(validation.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_v3_liquidity_update(
        &self,
        event: &DexEvent,
        state: &PoolState,
        update: &V3LiquidityUpdate,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO v3_liquidity_updates (
                id, pool_address, dex, variant, block_number, tx_hash, log_index,
                event_type, current_tick, tick_lower, tick_upper, amount,
                previous_liquidity, next_liquidity, created_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW())
            ON CONFLICT (pool_address, tx_hash, log_index) DO NOTHING
            "#,
        )
        .bind(address_to_string(event.pool_address))
        .bind(dex_to_string(state.dex))
        .bind(variant_to_string(state.variant))
        .bind(i64::try_from(event.block_number)?)
        .bind(&event.tx_hash)
        .bind(i64::try_from(event.log_index)?)
        .bind(&event.event_type)
        .bind(i64::from(update.current_tick))
        .bind(i64::from(update.tick_lower))
        .bind(i64::from(update.tick_upper))
        .bind(update.amount.to_string())
        .bind(update.previous_liquidity.to_string())
        .bind(update.next_liquidity.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn pool_block_has_any_event_types(
        &self,
        pool_address: Address,
        block_number: u64,
        event_types: &[&str],
    ) -> Result<bool> {
        if event_types.is_empty() {
            return Ok(false);
        }

        let event_types = event_types
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM dex_events
                WHERE lower(pool_address) = lower($1)
                  AND block_number = $2
                  AND event_type = ANY($3)
            )
            "#,
        )
        .bind(address_to_string(pool_address))
        .bind(i64::try_from(block_number)?)
        .bind(event_types)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }
}

#[derive(sqlx::FromRow)]
struct PendingTransactionRow {
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    eoa: String,
    tx_hash: String,
    nonce: i64,
}

#[async_trait]
impl PendingTransactionStore for PostgresStore {
    async fn pending_transactions_for_eoa(
        &self,
        eoa: Address,
        limit: i64,
    ) -> Result<Vec<PendingTransactionRecord>> {
        let rows: Vec<PendingTransactionRow> = sqlx::query_as(
            r#"
            SELECT opportunity_id, simulation_id, eoa, tx_hash, nonce
            FROM transactions
            WHERE eoa = $1
              AND status = 'Pending'
              AND tx_hash IS NOT NULL
            ORDER BY created_at ASC
            LIMIT $2
            "#,
        )
        .bind(address_to_string(eoa))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PendingTransactionRecord {
                    opportunity_id: row.opportunity_id,
                    simulation_id: row.simulation_id,
                    eoa: row.eoa.parse()?,
                    tx_hash: row.tx_hash.parse::<B256>()?,
                    nonce: u64::try_from(row.nonce)?,
                })
            })
            .collect()
    }

    async fn simulation_calldata(&self, simulation_id: uuid::Uuid) -> Result<Option<Vec<u8>>> {
        let raw: Option<String> = sqlx::query_scalar(
            r#"
            SELECT calldata
            FROM simulations
            WHERE id = $1
            "#,
        )
        .bind(simulation_id)
        .fetch_optional(&self.pool)
        .await?;

        raw.map(|value| {
            hex::decode(value.trim_start_matches("0x"))
                .map_err(|err| anyhow::anyhow!("invalid simulation calldata hex: {err}"))
        })
        .transpose()
    }
}

pub async fn ensure_registry_schema(pool: &PgPool) -> Result<()> {
    for statement in [
        r#"CREATE EXTENSION IF NOT EXISTS "uuid-ossp""#,
        r#"CREATE TABLE IF NOT EXISTS token_pairs (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            token0 TEXT NOT NULL,
            token1 TEXT NOT NULL,
            symbol TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, token0, token1)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS token_pairs_enabled_idx
            ON token_pairs (enabled, updated_at DESC)"#,
        r#"ALTER TABLE token_pairs
            ADD COLUMN IF NOT EXISTS token0_search_amounts TEXT"#,
        r#"ALTER TABLE token_pairs
            ADD COLUMN IF NOT EXISTS token1_search_amounts TEXT"#,
        r#"ALTER TABLE token_pairs
            ADD COLUMN IF NOT EXISTS token0_min_profit TEXT"#,
        r#"ALTER TABLE token_pairs
            ADD COLUMN IF NOT EXISTS token1_min_profit TEXT"#,
        r#"CREATE TABLE IF NOT EXISTS token_search_defaults (
            chain_id BIGINT NOT NULL,
            token_address TEXT NOT NULL,
            executor_scope TEXT NOT NULL DEFAULT 'all',
            search_amounts TEXT NOT NULL,
            min_profit TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chain_id, token_address, executor_scope)
        )"#,
        r#"ALTER TABLE token_search_defaults
            ADD COLUMN IF NOT EXISTS executor_scope TEXT NOT NULL DEFAULT 'all'"#,
        r#"ALTER TABLE token_search_defaults
            DROP CONSTRAINT IF EXISTS token_search_defaults_pkey"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS token_search_defaults_scope_unique_idx
            ON token_search_defaults (chain_id, token_address, executor_scope)"#,
        r#"CREATE TABLE IF NOT EXISTS pools (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            token_pair_id UUID REFERENCES token_pairs(id),
            chain_id BIGINT NOT NULL,
            pool_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            variant TEXT NOT NULL,
            token0 TEXT NOT NULL,
            token1 TEXT NOT NULL,
            fee_bps BIGINT,
            tick_spacing BIGINT,
            stable BOOLEAN,
            factory_address TEXT,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            source TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, pool_address)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS pools_enabled_idx
            ON pools (enabled, updated_at DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS pools_pair_idx
            ON pools (token_pair_id, enabled)"#,
        r#"ALTER TABLE pools
            ADD COLUMN IF NOT EXISTS factory_address TEXT"#,
        r#"CREATE INDEX IF NOT EXISTS dex_events_pool_block_type_idx
            ON dex_events (pool_address, block_number DESC, event_type)"#,
        r#"ALTER TABLE pool_states
            ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'unknown'"#,
        r#"CREATE TABLE IF NOT EXISTS pool_state_warnings (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            pool_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            variant TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            local_state_json JSONB NOT NULL,
            onchain_state_json JSONB NOT NULL,
            drift_bps BIGINT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
        r#"CREATE INDEX IF NOT EXISTS pool_state_warnings_created_idx
            ON pool_state_warnings (created_at DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS pool_state_warnings_pool_created_idx
            ON pool_state_warnings (pool_address, created_at DESC)"#,
        r#"CREATE TABLE IF NOT EXISTS pool_state_validations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            pool_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            variant TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            block_hash TEXT NOT NULL,
            local_state_json JSONB NOT NULL,
            onchain_state_json JSONB NOT NULL,
            drift_bps BIGINT NOT NULL,
            passed BOOLEAN NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (pool_address, block_number)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS pool_state_validations_created_idx
            ON pool_state_validations (created_at DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS pool_state_validations_pool_block_idx
            ON pool_state_validations (pool_address, block_number DESC)"#,
        r#"CREATE TABLE IF NOT EXISTS v3_liquidity_updates (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            pool_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            variant TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            tx_hash TEXT NOT NULL,
            log_index BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            current_tick BIGINT NOT NULL,
            tick_lower BIGINT NOT NULL,
            tick_upper BIGINT NOT NULL,
            amount TEXT NOT NULL,
            previous_liquidity TEXT NOT NULL,
            next_liquidity TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (pool_address, tx_hash, log_index)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS v3_liquidity_updates_pool_block_idx
            ON v3_liquidity_updates (pool_address, block_number DESC, log_index DESC)"#,
        r#"DELETE FROM transactions old
            USING transactions newer
            WHERE old.tx_hash IS NOT NULL
              AND old.tx_hash = newer.tx_hash
              AND (
                old.created_at < newer.created_at
                OR (old.created_at = newer.created_at AND old.id::text < newer.id::text)
              )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS transactions_tx_hash_unique_idx
            ON transactions (tx_hash)
            WHERE tx_hash IS NOT NULL"#,
        r#"CREATE TABLE IF NOT EXISTS observed_transactions (
            tx_hash TEXT PRIMARY KEY,
            block_number BIGINT NOT NULL,
            transaction_index BIGINT,
            from_address TEXT,
            to_address TEXT,
            nonce BIGINT,
            status BOOLEAN,
            gas_limit TEXT,
            gas_used TEXT,
            effective_gas_price TEXT,
            max_fee_per_gas TEXT,
            max_priority_fee_per_gas TEXT,
            tx_json JSONB NOT NULL,
            receipt_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
        r#"CREATE INDEX IF NOT EXISTS observed_transactions_block_idx
            ON observed_transactions (block_number, transaction_index)"#,
        r#"CREATE INDEX IF NOT EXISTS observed_transactions_tx_hash_lower_idx
            ON observed_transactions (lower(tx_hash))"#,
        r#"CREATE INDEX IF NOT EXISTS observed_transactions_from_idx
            ON observed_transactions (lower(from_address))"#,
        r#"CREATE INDEX IF NOT EXISTS observed_transactions_to_idx
            ON observed_transactions (lower(to_address))"#,
        r#"CREATE TABLE IF NOT EXISTS observed_blocks (
            block_number BIGINT PRIMARY KEY,
            block_hash TEXT,
            base_fee_per_gas TEXT,
            gas_used TEXT,
            gas_limit TEXT,
            block_timestamp BIGINT,
            tx_count BIGINT,
            block_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
        r#"CREATE TABLE IF NOT EXISTS observed_address_transfers (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            seed_address TEXT NOT NULL,
            direction TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            log_index BIGINT NOT NULL,
            token_address TEXT NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            counterparty TEXT NOT NULL,
            amount TEXT NOT NULL,
            raw_log_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (seed_address, direction, tx_hash, log_index)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS observed_address_transfers_seed_block_idx
            ON observed_address_transfers (lower(seed_address), block_number DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS observed_address_transfers_counterparty_idx
            ON observed_address_transfers (lower(counterparty))"#,
        r#"CREATE INDEX IF NOT EXISTS observed_address_transfers_tx_idx
            ON observed_address_transfers (lower(tx_hash))"#,
        r#"CREATE INDEX IF NOT EXISTS observed_address_transfers_seed_tx_idx
            ON observed_address_transfers (lower(seed_address), lower(tx_hash))"#,
        r#"CREATE TABLE IF NOT EXISTS tokens (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            token_address TEXT NOT NULL,
            symbol TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, token_address)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS tokens_enabled_idx
            ON tokens (chain_id, enabled, symbol)"#,
        r#"CREATE INDEX IF NOT EXISTS tokens_chain_lower_address_idx
            ON tokens (chain_id, lower(token_address))"#,
        r#"
            WITH computed AS (
                SELECT
                    tp.id,
                    COALESCE(NULLIF(BTRIM(t0.symbol), ''), left(tp.token0, 10))
                        || '-' || right(tp.token0, 6) || '/' ||
                    COALESCE(NULLIF(BTRIM(t1.symbol), ''), left(tp.token1, 10))
                        || '-' || right(tp.token1, 6) AS new_symbol
                FROM token_pairs tp
                LEFT JOIN tokens t0
                    ON t0.chain_id = tp.chain_id
                   AND lower(t0.token_address) = lower(tp.token0)
                LEFT JOIN tokens t1
                    ON t1.chain_id = tp.chain_id
                   AND lower(t1.token_address) = lower(tp.token1)
            )
            UPDATE token_pairs tp
            SET symbol = computed.new_symbol
            FROM computed
            WHERE tp.id = computed.id
              AND tp.symbol IS DISTINCT FROM computed.new_symbol
        "#,
        r#"CREATE TABLE IF NOT EXISTS observed_pools (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            pool_address TEXT NOT NULL,
            topic0 TEXT NOT NULL,
            family TEXT NOT NULL,
            token0 TEXT,
            token1 TEXT,
            symbol TEXT,
            factory_address TEXT,
            dex TEXT,
            variant TEXT,
            fee_bps BIGINT,
            fee_pips BIGINT,
            pool_key_fee_pips BIGINT,
            tick_spacing BIGINT,
            stable BOOLEAN,
            txs_30d BIGINT NOT NULL DEFAULT 0,
            logs_30d BIGINT NOT NULL DEFAULT 0,
            first_block BIGINT,
            latest_block BIGINT,
            discovery_source TEXT NOT NULL DEFAULT 'unknown',
            import_status TEXT NOT NULL,
            import_reason TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, pool_address)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS observed_pools_status_idx
            ON observed_pools (chain_id, import_status, txs_30d DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS observed_pools_symbol_idx
            ON observed_pools (chain_id, symbol, txs_30d DESC)"#,
        r#"ALTER TABLE observed_pools
            ADD COLUMN IF NOT EXISTS discovery_source TEXT NOT NULL DEFAULT 'unknown'"#,
        r#"CREATE INDEX IF NOT EXISTS observed_pools_source_idx
            ON observed_pools (chain_id, discovery_source, updated_at DESC)"#,
        r#"CREATE TABLE IF NOT EXISTS protocol_pool_observations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            protocol TEXT NOT NULL,
            manager_address TEXT NOT NULL,
            pool_uid TEXT NOT NULL,
            pool_address TEXT,
            topic0 TEXT NOT NULL,
            event_type TEXT NOT NULL,
            token0 TEXT,
            token1 TEXT,
            symbol TEXT,
            factory_address TEXT,
            dex TEXT,
            variant TEXT,
            fee_bps BIGINT,
            fee_pips BIGINT,
            tick_spacing BIGINT,
            hooks_address TEXT,
            sqrt_price_x96 TEXT,
            liquidity TEXT,
            tick BIGINT,
            first_block BIGINT NOT NULL,
            latest_block BIGINT NOT NULL,
            logs_30d BIGINT NOT NULL DEFAULT 0,
            discovery_source TEXT NOT NULL,
            import_status TEXT NOT NULL,
            import_reason TEXT,
            raw_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, protocol, manager_address, pool_uid)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS protocol_pool_observations_status_idx
            ON protocol_pool_observations (chain_id, protocol, import_status, logs_30d DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS protocol_pool_observations_latest_idx
            ON protocol_pool_observations (chain_id, latest_block DESC, updated_at DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS protocol_pool_observations_pool_address_idx
            ON protocol_pool_observations (chain_id, lower(pool_address))
            WHERE pool_address IS NOT NULL"#,
        r#"ALTER TABLE protocol_pool_observations
            ADD COLUMN IF NOT EXISTS pool_key_fee_pips BIGINT"#,
        r#"CREATE TABLE IF NOT EXISTS pool_ticks_current (
            chain_id BIGINT NOT NULL,
            pool_address TEXT NOT NULL,
            tick INTEGER NOT NULL,
            liquidity_net TEXT NOT NULL,
            liquidity_gross TEXT NOT NULL,
            block_number BIGINT NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (chain_id, pool_address, tick)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS pool_ticks_current_pool_idx
            ON pool_ticks_current (chain_id, lower(pool_address))"#,
        r#"CREATE TABLE IF NOT EXISTS pool_tick_hydration_runs (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            protocol TEXT NOT NULL,
            manager_address TEXT,
            from_block BIGINT NOT NULL,
            to_block BIGINT NOT NULL,
            selected_pools BIGINT NOT NULL,
            hydrated_pools BIGINT NOT NULL DEFAULT 0,
            ticks_written BIGINT NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ
        )"#,
        r#"CREATE INDEX IF NOT EXISTS pool_tick_hydration_runs_status_idx
            ON pool_tick_hydration_runs (chain_id, protocol, status, started_at DESC)"#,
        r#"CREATE TABLE IF NOT EXISTS factory_registry (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            chain_id BIGINT NOT NULL,
            factory_address TEXT NOT NULL,
            dex TEXT NOT NULL,
            variant TEXT NOT NULL,
            trusted BOOLEAN NOT NULL DEFAULT FALSE,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            source TEXT NOT NULL,
            notes TEXT,
            observed_pools BIGINT NOT NULL DEFAULT 0,
            first_seen_block BIGINT,
            latest_seen_block BIGINT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (chain_id, factory_address)
        )"#,
        r#"CREATE INDEX IF NOT EXISTS factory_registry_trusted_idx
            ON factory_registry (chain_id, trusted, enabled, updated_at DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS factory_registry_observed_idx
            ON factory_registry (chain_id, observed_pools DESC, updated_at DESC)"#,
        r#"ALTER TABLE simulations
            ADD COLUMN IF NOT EXISTS block_number BIGINT,
            ADD COLUMN IF NOT EXISTS token_in TEXT,
            ADD COLUMN IF NOT EXISTS amount_in TEXT,
            ADD COLUMN IF NOT EXISTS expected_profit TEXT,
            ADD COLUMN IF NOT EXISTS min_profit TEXT,
            ADD COLUMN IF NOT EXISTS path_name TEXT,
            ADD COLUMN IF NOT EXISTS base_fee_per_gas TEXT,
            ADD COLUMN IF NOT EXISTS max_fee_per_gas TEXT,
            ADD COLUMN IF NOT EXISTS max_priority_fee_per_gas TEXT,
            ADD COLUMN IF NOT EXISTS gas_cost_cap TEXT,
            ADD COLUMN IF NOT EXISTS gas_cost_expected TEXT,
            ADD COLUMN IF NOT EXISTS net_simulated_profit TEXT"#,
        r#"CREATE INDEX IF NOT EXISTS simulations_block_idx
            ON simulations (block_number DESC)"#,
        r#"CREATE INDEX IF NOT EXISTS simulations_path_created_idx
            ON simulations (path_name, created_at DESC)"#,
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

#[derive(sqlx::FromRow)]
struct PoolRegistryRow {
    pool_address: String,
    dex: String,
    variant: String,
    factory_address: Option<String>,
    token0: String,
    token1: String,
    fee_bps: Option<i64>,
    tick_spacing: Option<i64>,
    stable: Option<bool>,
    enabled: bool,
}

#[derive(sqlx::FromRow)]
struct TokenPairSearchConfigRow {
    chain_id: i64,
    token0: String,
    token1: String,
    symbol: String,
    token0_search_amounts: Option<String>,
    token1_search_amounts: Option<String>,
    token0_multihop_search_amounts: Option<String>,
    token1_multihop_search_amounts: Option<String>,
    token0_min_profit: Option<String>,
    token1_min_profit: Option<String>,
    token0_multihop_min_profit: Option<String>,
    token1_multihop_min_profit: Option<String>,
    token0_all_min_profit: Option<String>,
    token1_all_min_profit: Option<String>,
}

impl TryFrom<PoolRegistryRow> for PoolRegistryEntry {
    type Error = anyhow::Error;

    fn try_from(row: PoolRegistryRow) -> Result<Self> {
        Ok(Self {
            pool_address: row.pool_address.parse()?,
            dex: parse_dex(&row.dex)?,
            variant: parse_variant(&row.variant)?,
            factory_address: row.factory_address.as_deref().map(str::parse).transpose()?,
            token0: row.token0.parse()?,
            token1: row.token1.parse()?,
            fee_bps: u32::try_from(row.fee_bps.unwrap_or_default())?,
            tick_spacing: row.tick_spacing.map(i32::try_from).transpose()?,
            stable: row.stable,
            enabled: row.enabled,
        })
    }
}

impl TryFrom<TokenPairSearchConfigRow> for TokenPairSearchConfig {
    type Error = anyhow::Error;

    fn try_from(row: TokenPairSearchConfigRow) -> Result<Self> {
        Ok(Self {
            chain_id: u64::try_from(row.chain_id)?,
            token0: row.token0.parse()?,
            token1: row.token1.parse()?,
            symbol: row.symbol,
            token0_search_amounts: parse_raw_amount_list(row.token0_search_amounts.as_deref())?,
            token1_search_amounts: parse_raw_amount_list(row.token1_search_amounts.as_deref())?,
            token0_multihop_search_amounts: parse_raw_amount_list(
                row.token0_multihop_search_amounts.as_deref(),
            )?,
            token1_multihop_search_amounts: parse_raw_amount_list(
                row.token1_multihop_search_amounts.as_deref(),
            )?,
            token0_min_profit: effective_min_profit(
                row.token0_min_profit.as_deref(),
                row.token0_all_min_profit.as_deref(),
            )?,
            token1_min_profit: effective_min_profit(
                row.token1_min_profit.as_deref(),
                row.token1_all_min_profit.as_deref(),
            )?,
            token0_multihop_min_profit: effective_min_profit(
                row.token0_multihop_min_profit.as_deref(),
                row.token0_all_min_profit.as_deref(),
            )?,
            token1_multihop_min_profit: effective_min_profit(
                row.token1_multihop_min_profit.as_deref(),
                row.token1_all_min_profit.as_deref(),
            )?,
        })
    }
}

fn effective_min_profit(raw: Option<&str>, all_raw: Option<&str>) -> Result<U256> {
    let min_profit = parse_raw_amount(raw)?.unwrap_or(U256::from(1u64));
    let all_min_profit = parse_raw_amount(all_raw)?.unwrap_or(U256::ZERO);
    Ok(min_profit.max(all_min_profit))
}

#[async_trait]
impl PairSearchConfigStore for PostgresStore {
    async fn enabled_pair_search_configs(&self) -> Result<Vec<TokenPairSearchConfig>> {
        PostgresStore::enabled_pair_search_configs(self).await
    }
}

fn parse_raw_amount_list(raw: Option<&str>) -> Result<Vec<U256>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(parse_raw_amount(Some(trimmed)).and_then(|value| {
                    value.ok_or_else(|| anyhow::anyhow!("empty raw amount in list"))
                }))
            }
        })
        .collect()
}

fn parse_raw_amount(raw: Option<&str>) -> Result<Option<U256>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(U256::from_str_radix(raw, 10)?))
}

fn normalize_executor_scope(scope: &str) -> Result<&'static str> {
    match scope.trim() {
        "" | "all" => Ok("all"),
        "two_hop" | "2hop" | "2-hop" => Ok("two_hop"),
        "multihop" | "multi_hop" | "multi-hop" => Ok("multihop"),
        other => anyhow::bail!("invalid executor scope: {other}"),
    }
}

#[async_trait]
impl RecorderStore for PostgresStore {
    async fn record_dex_event(&self, event: DexEvent) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO dex_events (
                id, chain_id, block_number, tx_hash, log_index, pool_address, dex,
                event_type, token0, token1, raw_data_json, created_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (chain_id, tx_hash, log_index) DO NOTHING
            "#,
        )
        // $1..$10 map exactly to the non-id/created_at insert columns above.
        .bind(8453_i64)
        .bind(i64::try_from(event.block_number)?)
        .bind(event.tx_hash)
        .bind(i64::try_from(event.log_index)?)
        .bind(address_to_string(event.pool_address))
        .bind(dex_to_string(event.dex))
        .bind(event.event_type)
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(sqlx::types::Json(event.raw_data_json))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_pool_state(&self, pool_state: PoolState) -> Result<()> {
        self.record_pool_state_with_source(pool_state, "unknown")
            .await
    }

    async fn record_pool_state_with_source(
        &self,
        pool_state: PoolState,
        source: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO pool_states (
                id, pool_address, dex, token0, token1, fee, reserve0, reserve1,
                sqrt_price_x96, liquidity, tick, block_number, updated_at, source
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(address_to_string(pool_state.pool_id.address))
        .bind(dex_to_string(pool_state.dex))
        .bind(address_to_string(pool_state.token0))
        .bind(address_to_string(pool_state.token1))
        .bind(i64::from(pool_state.fee_bps))
        .bind(pool_state.reserve0.map(|v| v.to_string()))
        .bind(pool_state.reserve1.map(|v| v.to_string()))
        .bind(pool_state.sqrt_price_x96.map(|v| v.to_string()))
        .bind(pool_state.liquidity.map(|v| v.to_string()))
        .bind(pool_state.tick.map(i64::from))
        .bind(i64::try_from(pool_state.block_number)?)
        .bind(pool_state.updated_at)
        .bind(source)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_opportunity(&self, candidate: Candidate) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO opportunities (
                id, created_at, block_number, strategy, token_in, amount_in,
                expected_amount_out, expected_profit, min_profit, path_json, status
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(candidate.id)
        .bind(candidate.created_at)
        .bind(i64::try_from(candidate.block_number)?)
        .bind(candidate.strategy)
        .bind(address_to_string(candidate.token_in))
        .bind(candidate.amount_in.to_string())
        .bind(candidate.expected_amount_out.to_string())
        .bind(candidate.expected_profit.to_string())
        .bind(candidate.min_profit.to_string())
        .bind(sqlx::types::Json(candidate.path))
        .bind(format!("{:?}", candidate.status))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_opportunities(&self, candidates: Vec<Candidate>) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let block_number = i64::try_from(candidate.block_number)?;
            rows.push((candidate, block_number));
        }
        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO opportunities (
                id, created_at, block_number, strategy, token_in, amount_in,
                expected_amount_out, expected_profit, min_profit, path_json, status
            )
            "#,
        );
        query.push_values(rows, |mut row, (candidate, block_number)| {
            row.push_bind(candidate.id)
                .push_bind(candidate.created_at)
                .push_bind(block_number)
                .push_bind(candidate.strategy)
                .push_bind(address_to_string(candidate.token_in))
                .push_bind(candidate.amount_in.to_string())
                .push_bind(candidate.expected_amount_out.to_string())
                .push_bind(candidate.expected_profit.to_string())
                .push_bind(candidate.min_profit.to_string())
                .push_bind(sqlx::types::Json(candidate.path))
                .push_bind(format!("{:?}", candidate.status));
        });
        query.push(" ON CONFLICT (id) DO NOTHING");
        query.build().execute(&self.pool).await?;
        Ok(())
    }

    async fn record_simulation(&self, simulation: SimulationResult) -> Result<()> {
        let calldata_hex = format!("0x{}", hex::encode(&simulation.calldata));
        sqlx::query(
            r#"
            INSERT INTO simulations (
                id, opportunity_id, created_at, success, simulated_profit, gas_estimate,
                revert_reason, calldata, raw_result, block_number, token_in, amount_in,
                expected_profit, min_profit, path_name, base_fee_per_gas, max_fee_per_gas,
                max_priority_fee_per_gas, gas_cost_cap, gas_cost_expected, net_simulated_profit
            ) VALUES ($1,$2,NOW(),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
            "#,
        )
        .bind(simulation.id)
        .bind(simulation.opportunity_id)
        .bind(simulation.success)
        .bind(simulation.simulated_profit.to_string())
        .bind(simulation.gas_estimate.map(|v| v.to_string()))
        .bind(simulation.revert_reason.clone())
        .bind(calldata_hex)
        .bind(sqlx::types::Json(serde_json::json!({
            "success": simulation.success,
            "gas_estimate": simulation.gas_estimate.map(|v| v.to_string()),
            "block_number": simulation.block_number,
            "token_in": simulation.token_in.map(address_to_string),
            "amount_in": simulation.amount_in.map(|v| v.to_string()),
            "expected_profit": simulation.expected_profit.map(|v| v.to_string()),
            "min_profit": simulation.min_profit.map(|v| v.to_string()),
            "path_name": simulation.path_name.clone(),
            "base_fee_per_gas": simulation.base_fee_per_gas.map(|v| v.to_string()),
            "max_fee_per_gas": simulation.max_fee_per_gas.map(|v| v.to_string()),
            "max_priority_fee_per_gas": simulation.max_priority_fee_per_gas.map(|v| v.to_string()),
            "gas_cost_cap": simulation.gas_cost_cap.map(|v| v.to_string()),
            "gas_cost_expected": simulation.gas_cost_expected.map(|v| v.to_string()),
            "net_simulated_profit": simulation.net_simulated_profit.map(|v| v.to_string()),
        })))
        .bind(
            simulation
                .block_number
                .map(|v| i64::try_from(v))
                .transpose()?,
        )
        .bind(simulation.token_in.map(address_to_string))
        .bind(simulation.amount_in.map(|v| v.to_string()))
        .bind(simulation.expected_profit.map(|v| v.to_string()))
        .bind(simulation.min_profit.map(|v| v.to_string()))
        .bind(simulation.path_name.clone())
        .bind(simulation.base_fee_per_gas.map(|v| v.to_string()))
        .bind(simulation.max_fee_per_gas.map(|v| v.to_string()))
        .bind(simulation.max_priority_fee_per_gas.map(|v| v.to_string()))
        .bind(simulation.gas_cost_cap.map(|v| v.to_string()))
        .bind(simulation.gas_cost_expected.map(|v| v.to_string()))
        .bind(simulation.net_simulated_profit.map(|v| v.to_string()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_transaction(&self, tx: TxResult) -> Result<()> {
        let tx_hash = tx.tx_hash.map(|v| format!("{v:#x}"));
        if tx_hash.is_some() {
            sqlx::query(
                r#"
                INSERT INTO transactions (
                    id, opportunity_id, simulation_id, created_at, eoa, tx_hash, nonce, status,
                    gas_used, effective_gas_price, realized_profit, revert_reason, receipt_json
                ) VALUES ($1,$2,$3,NOW(),$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (tx_hash) WHERE tx_hash IS NOT NULL
                DO UPDATE SET
                    opportunity_id = EXCLUDED.opportunity_id,
                    simulation_id = COALESCE(EXCLUDED.simulation_id, transactions.simulation_id),
                    eoa = EXCLUDED.eoa,
                    nonce = EXCLUDED.nonce,
                    status = EXCLUDED.status,
                    gas_used = EXCLUDED.gas_used,
                    effective_gas_price = EXCLUDED.effective_gas_price,
                    realized_profit = EXCLUDED.realized_profit,
                    revert_reason = EXCLUDED.revert_reason,
                    receipt_json = EXCLUDED.receipt_json,
                    created_at = EXCLUDED.created_at
                "#,
            )
            .bind(uuid::Uuid::new_v4())
            .bind(tx.opportunity_id)
            .bind(tx.simulation_id)
            .bind(address_to_string(tx.eoa))
            .bind(tx_hash)
            .bind(i64::try_from(tx.nonce)?)
            .bind(format!("{:?}", tx.status))
            .bind(tx.gas_used.map(|v| v.to_string()))
            .bind(tx.effective_gas_price.map(|v| v.to_string()))
            .bind(tx.realized_profit.map(|v| v.to_string()))
            .bind(tx.revert_reason)
            .bind(sqlx::types::Json(
                tx.receipt_json.unwrap_or_else(|| serde_json::json!({})),
            ))
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO transactions (
                    id, opportunity_id, simulation_id, created_at, eoa, tx_hash, nonce, status,
                    gas_used, effective_gas_price, realized_profit, revert_reason, receipt_json
                ) VALUES ($1,$2,$3,NOW(),$4,$5,$6,$7,$8,$9,$10,$11,$12)
                "#,
            )
            .bind(uuid::Uuid::new_v4())
            .bind(tx.opportunity_id)
            .bind(tx.simulation_id)
            .bind(address_to_string(tx.eoa))
            .bind(tx_hash)
            .bind(i64::try_from(tx.nonce)?)
            .bind(format!("{:?}", tx.status))
            .bind(tx.gas_used.map(|v| v.to_string()))
            .bind(tx.effective_gas_price.map(|v| v.to_string()))
            .bind(tx.realized_profit.map(|v| v.to_string()))
            .bind(tx.revert_reason)
            .bind(sqlx::types::Json(
                tx.receipt_json.unwrap_or_else(|| serde_json::json!({})),
            ))
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }
}

fn address_to_string(address: Address) -> String {
    format!("{address:#x}")
}

fn dex_to_string(dex: DexKind) -> &'static str {
    match dex {
        DexKind::Aerodrome => "Aerodrome",
        DexKind::UniswapV3 => "UniswapV3",
        DexKind::PancakeSwap => "PancakeSwap",
        DexKind::UniswapV4 => "UniswapV4",
        DexKind::Balancer => "Balancer",
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
        PoolVariant::PancakeV3 => "PancakeV3",
        PoolVariant::UniswapV4 => "UniswapV4",
        PoolVariant::BalancerV3 => "BalancerV3",
    }
}

fn parse_dex(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        "UniswapV4" => Ok(DexKind::UniswapV4),
        "Balancer" => Ok(DexKind::Balancer),
        _ => anyhow::bail!("unknown dex kind {value}"),
    }
}

fn parse_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        "UniswapV4" => Ok(PoolVariant::UniswapV4),
        "BalancerV3" => Ok(PoolVariant::BalancerV3),
        _ => anyhow::bail!("unknown pool variant {value}"),
    }
}
