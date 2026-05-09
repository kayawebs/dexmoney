use alloy_primitives::U256;
use anyhow::{Context, Result};
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{DexKind, PoolState, PoolVariant};
use chrono::Utc;
use tracing::info;

const SYNC_TOPIC: &str = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";

pub fn log_pool_state_update(pool_state: &PoolState) {
    info!(
        pool = %pool_state.pool_id.address,
        block_number = pool_state.block_number,
        "pool state updated"
    );
}

pub fn apply_event_to_pool_state(state: &mut PoolState, event: &DexEvent) -> Result<bool> {
    if state.pool_id.address != event.pool_address {
        return Ok(false);
    }
    if state.dex != DexKind::Aerodrome || state.variant != PoolVariant::AerodromeVolatile {
        return Ok(false);
    }

    let Some(topic0) = event
        .raw_data_json
        .get("topics")
        .and_then(|topics| topics.get(0))
        .and_then(|topic| topic.as_str())
    else {
        return Ok(false);
    };
    if topic0 != SYNC_TOPIC {
        return Ok(false);
    }

    let data = event
        .raw_data_json
        .get("data")
        .and_then(|data| data.as_str())
        .context("Sync event missing data")?;
    let (reserve0, reserve1) = decode_sync_reserves(data)?;

    state.reserve0 = Some(reserve0);
    state.reserve1 = Some(reserve1);
    state.block_number = event.block_number;
    state.updated_at = Utc::now();
    Ok(true)
}

fn decode_sync_reserves(data: &str) -> Result<(U256, U256)> {
    let clean = data.trim_start_matches("0x");
    if clean.len() < 128 {
        anyhow::bail!("Sync data too short");
    }
    let reserve0 = U256::from_str_radix(&clean[0..64], 16)?;
    let reserve1 = U256::from_str_radix(&clean[64..128], 16)?;
    Ok((reserve0, reserve1))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::Utc;
    use serde_json::json;

    use base_arb_chain::events::DexEvent;
    use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};

    use super::apply_event_to_pool_state;

    #[test]
    fn applies_aerodrome_sync_reserves() {
        let pool = address!("1111111111111111111111111111111111111111");
        let mut state = PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: DexKind::Aerodrome,
            variant: PoolVariant::AerodromeVolatile,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            fee_bps: 30,
            reserve0: Some(U256::from(1u64)),
            reserve1: Some(U256::from(2u64)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            block_number: 1,
            updated_at: Utc::now(),
        };
        let event = DexEvent {
            block_number: 9,
            tx_hash: "0xabc".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::Aerodrome,
            event_type: "Sync".into(),
            raw_data_json: json!({
                "topics": [super::SYNC_TOPIC],
                "data": "0x000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000014"
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(changed);
        assert_eq!(state.reserve0, Some(U256::from(10u64)));
        assert_eq!(state.reserve1, Some(U256::from(20u64)));
        assert_eq!(state.block_number, 9);
    }
}
