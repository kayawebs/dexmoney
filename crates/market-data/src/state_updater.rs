use alloy_primitives::U256;
use anyhow::{Context, Result};
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{DexKind, PoolState, PoolVariant};
use chrono::Utc;
use tracing::info;

const SYNC_TOPIC: &str = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

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
    let Some(topic0) = event
        .raw_data_json
        .get("topics")
        .and_then(|topics| topics.get(0))
        .and_then(|topic| topic.as_str())
    else {
        return Ok(false);
    };
    let data = event
        .raw_data_json
        .get("data")
        .and_then(|data| data.as_str())
        .context("pool event missing data")?;

    if state.dex == DexKind::Aerodrome
        && state.variant == PoolVariant::AerodromeVolatile
        && topic0 == SYNC_TOPIC
    {
        let (reserve0, reserve1) = decode_sync_reserves(data)?;
        state.reserve0 = Some(reserve0);
        state.reserve1 = Some(reserve1);
    } else if is_v3_style(state) && topic0 == V3_SWAP_TOPIC {
        let (sqrt_price_x96, liquidity, tick) = decode_v3_swap_state(data)?;
        state.sqrt_price_x96 = Some(sqrt_price_x96);
        state.liquidity = Some(liquidity);
        state.tick = Some(tick);
    } else {
        return Ok(false);
    }

    state.block_number = event.block_number;
    state.updated_at = Utc::now();
    Ok(true)
}

fn is_v3_style(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
            | (DexKind::UniswapV3, PoolVariant::UniswapV3)
    )
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

fn decode_v3_swap_state(data: &str) -> Result<(U256, U256, i32)> {
    let clean = data.trim_start_matches("0x");
    if clean.len() < 320 {
        anyhow::bail!("V3 Swap data too short");
    }
    let sqrt_price_x96 = U256::from_str_radix(&clean[128..192], 16)?;
    let liquidity = U256::from_str_radix(&clean[192..256], 16)?;
    let tick = decode_i24_word(&clean[256..320])?;
    Ok((sqrt_price_x96, liquidity, tick))
}

fn decode_i24_word(word: &str) -> Result<i32> {
    if word.len() != 64 {
        anyhow::bail!("int24 ABI word must be 32 bytes");
    }
    let low = u32::from_str_radix(&word[word.len() - 6..], 16)?;
    if (low & 0x800000) != 0 {
        Ok((low as i32) - (1 << 24))
    } else {
        Ok(low as i32)
    }
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

    #[test]
    fn applies_uniswap_v3_swap_state() {
        let pool = address!("2222222222222222222222222222222222222222");
        let mut state = PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            fee_bps: 5,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(1u64)),
            liquidity: Some(U256::from(2u64)),
            tick: Some(3),
            block_number: 1,
            updated_at: Utc::now(),
        };
        let event = DexEvent {
            block_number: 10,
            tx_hash: "0xdef".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::UniswapV3,
            event_type: "Swap".into(),
            raw_data_json: json!({
                "topics": [super::V3_SWAP_TOPIC],
                "data": concat!(
                    "0x",
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "0000000000000000000000000000000000000000000000000000000000000001",
                    "000000000000000000000000000000000000000000000000000000000000007b",
                    "00000000000000000000000000000000000000000000000000000000000001c8",
                    "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcf1da"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(changed);
        assert_eq!(state.sqrt_price_x96, Some(U256::from(123u64)));
        assert_eq!(state.liquidity, Some(U256::from(456u64)));
        assert_eq!(state.tick, Some(-200_230));
        assert_eq!(state.block_number, 10);
    }

    #[test]
    fn applies_aerodrome_slipstream_swap_state() {
        let pool = address!("3333333333333333333333333333333333333333");
        let mut state = PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: DexKind::Aerodrome,
            variant: PoolVariant::AerodromeSlipstream,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            fee_bps: 30,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            block_number: 1,
            updated_at: Utc::now(),
        };
        let event = DexEvent {
            block_number: 11,
            tx_hash: "0xghi".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::Aerodrome,
            event_type: "Swap".into(),
            raw_data_json: json!({
                "topics": [super::V3_SWAP_TOPIC],
                "data": concat!(
                    "0x",
                    "0000000000000000000000000000000000000000000000000000000000000001",
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "00000000000000000000000000000000000000000000000000000000000003e7",
                    "000000000000000000000000000000000000000000000000000000000000022b",
                    "000000000000000000000000000000000000000000000000000000000000002a"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(changed);
        assert_eq!(state.sqrt_price_x96, Some(U256::from(999u64)));
        assert_eq!(state.liquidity, Some(U256::from(555u64)));
        assert_eq!(state.tick, Some(42));
        assert_eq!(state.block_number, 11);
    }
}
