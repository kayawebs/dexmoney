use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tracing::info;

use crate::RecorderStore;
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{
    Candidate, DexKind, DiscoveredPool, PoolRegistryEntry, PoolState, PoolStateWarning,
    PoolVariant, SimulationResult, TxResult, V3LiquidityUpdate,
};

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
                fee_bps, tick_spacing, stable, enabled, source, created_at, updated_at
            ) VALUES (uuid_generate_v4(), $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,TRUE,$11,NOW(),NOW())
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
        .bind(&discovered.source)
        .execute(&self.pool)
        .await?;
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
                pool_address, dex, variant, token0, token1, fee_bps, tick_spacing, stable, enabled
            FROM pools
            WHERE enabled = TRUE
            ORDER BY lower(pool_address), updated_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(PoolRegistryEntry::try_from).collect()
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

        let event_types = event_types.iter().map(|value| value.to_string()).collect::<Vec<_>>();
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
    token0: String,
    token1: String,
    fee_bps: Option<i64>,
    tick_spacing: Option<i64>,
    stable: Option<bool>,
    enabled: bool,
}

impl TryFrom<PoolRegistryRow> for PoolRegistryEntry {
    type Error = anyhow::Error;

    fn try_from(row: PoolRegistryRow) -> Result<Self> {
        Ok(Self {
            pool_address: row.pool_address.parse()?,
            dex: parse_dex(&row.dex)?,
            variant: parse_variant(&row.variant)?,
            token0: row.token0.parse()?,
            token1: row.token1.parse()?,
            fee_bps: u32::try_from(row.fee_bps.unwrap_or_default())?,
            tick_spacing: row.tick_spacing.map(i32::try_from).transpose()?,
            stable: row.stable,
            enabled: row.enabled,
        })
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

    async fn record_simulation(&self, simulation: SimulationResult) -> Result<()> {
        let calldata_hex = format!("0x{}", hex::encode(&simulation.calldata));
        sqlx::query(
            r#"
            INSERT INTO simulations (
                id, opportunity_id, created_at, success, simulated_profit, gas_estimate,
                revert_reason, calldata, raw_result
            ) VALUES ($1,$2,NOW(),$3,$4,$5,$6,$7,$8)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(simulation.opportunity_id)
        .bind(simulation.success)
        .bind(simulation.simulated_profit.to_string())
        .bind(simulation.gas_estimate.map(|v| v.to_string()))
        .bind(simulation.revert_reason.clone())
        .bind(calldata_hex)
        .bind(sqlx::types::Json(serde_json::json!({
            "success": simulation.success,
            "gas_estimate": simulation.gas_estimate.map(|v| v.to_string()),
        })))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_transaction(&self, tx: TxResult) -> Result<()> {
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
        .bind("0x0000000000000000000000000000000000000000")
        .bind(tx.tx_hash.map(|v| format!("{v:#x}")))
        .bind(i64::try_from(tx.nonce)?)
        .bind(format!("{:?}", tx.status))
        .bind(tx.gas_used.map(|v| v.to_string()))
        .bind(tx.effective_gas_price.map(|v| v.to_string()))
        .bind(tx.realized_profit.map(|v| v.to_string()))
        .bind(tx.revert_reason)
        .bind(sqlx::types::Json(serde_json::json!({})))
        .execute(&self.pool)
        .await?;
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
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
    }
}

fn parse_dex(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        _ => anyhow::bail!("unknown dex kind {value}"),
    }
}

fn parse_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        _ => anyhow::bail!("unknown pool variant {value}"),
    }
}
