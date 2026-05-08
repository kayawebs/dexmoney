use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tracing::info;

use crate::RecorderStore;
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{Candidate, DexKind, PoolState, SimulationResult, TxResult};

#[derive(Clone)]
pub struct PostgresStore {
    pub pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        info!("connected to postgres");
        Ok(Self { pool })
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
        sqlx::query(
            r#"
            INSERT INTO pool_states (
                id, pool_address, dex, token0, token1, fee, reserve0, reserve1,
                sqrt_price_x96, liquidity, tick, block_number, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
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
