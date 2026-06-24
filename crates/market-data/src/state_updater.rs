use alloy_primitives::U256;
use anyhow::{Context, Result};
use base_arb_chain::events::DexEvent;
use base_arb_common::types::{DexKind, PoolState, PoolVariant};
use chrono::Utc;
use tracing::debug;

const AERODROME_SYNC_TOPIC: &str =
    "0xcf2aa50876cdfbb541206f89af0ee78d44a2abf8d328e37fa4917f982149848a";
const UNISWAP_V2_SYNC_TOPIC: &str =
    "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const V3_MINT_TOPIC: &str = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde";
const V3_BURN_TOPIC: &str = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickDelta {
    pub tick: i32,
    pub liquidity_gross_delta: i128,
    pub liquidity_net_delta: i128,
}

pub fn log_pool_state_update(pool_state: &PoolState) {
    debug!(
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
        && (topic0 == AERODROME_SYNC_TOPIC || topic0 == UNISWAP_V2_SYNC_TOPIC)
    {
        let (reserve0, reserve1) = decode_sync_reserves(data)?;
        state.reserve0 = Some(reserve0);
        state.reserve1 = Some(reserve1);
    } else if is_v3_style(state)
        && (topic0 == V3_SWAP_TOPIC
            || (state.variant == PoolVariant::PancakeV3 && topic0 == PANCAKE_V3_SWAP_TOPIC))
    {
        let (sqrt_price_x96, liquidity, tick) = decode_v3_swap_state(data)?;
        state.sqrt_price_x96 = Some(sqrt_price_x96);
        state.liquidity = Some(liquidity);
        state.tick = Some(tick);
    } else {
        return Ok(false);
    }

    state.block_number = event.block_number;
    state.valid_through_block = state.valid_through_block.max(event.block_number);
    state.updated_at = Utc::now();
    Ok(true)
}

pub fn is_v3_liquidity_event(state: &PoolState, event: &DexEvent) -> Result<bool> {
    if state.pool_id.address != event.pool_address || !is_v3_style(state) {
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
    Ok(topic0 == V3_MINT_TOPIC || topic0 == V3_BURN_TOPIC)
}

pub fn v3_tick_deltas_from_event(state: &PoolState, event: &DexEvent) -> Result<Vec<TickDelta>> {
    if state.pool_id.address != event.pool_address || !is_v3_style(state) {
        return Ok(Vec::new());
    }
    let Some(topic0) = event
        .raw_data_json
        .get("topics")
        .and_then(|topics| topics.get(0))
        .and_then(|topic| topic.as_str())
    else {
        return Ok(Vec::new());
    };
    if topic0 != V3_MINT_TOPIC && topic0 != V3_BURN_TOPIC {
        return Ok(Vec::new());
    }
    let topics = event
        .raw_data_json
        .get("topics")
        .and_then(|topics| topics.as_array())
        .context("pool event missing topics")?;
    if topics.len() < 4 {
        anyhow::bail!("V3 Mint/Burn event missing indexed tick topics");
    }
    let data = event
        .raw_data_json
        .get("data")
        .and_then(|data| data.as_str())
        .context("pool event missing data")?;
    let tick_lower = decode_i24_topic(&topics[2])?;
    let tick_upper = decode_i24_topic(&topics[3])?;
    let amount = decode_v3_liquidity_amount(data, topic0 == V3_MINT_TOPIC)?;
    let amount: i128 = amount
        .try_into()
        .map_err(|_| anyhow::anyhow!("liquidity amount does not fit i128"))?;

    if topic0 == V3_MINT_TOPIC {
        Ok(vec![
            TickDelta {
                tick: tick_lower,
                liquidity_gross_delta: amount,
                liquidity_net_delta: amount,
            },
            TickDelta {
                tick: tick_upper,
                liquidity_gross_delta: amount,
                liquidity_net_delta: -amount,
            },
        ])
    } else {
        Ok(vec![
            TickDelta {
                tick: tick_lower,
                liquidity_gross_delta: -amount,
                liquidity_net_delta: -amount,
            },
            TickDelta {
                tick: tick_upper,
                liquidity_gross_delta: -amount,
                liquidity_net_delta: amount,
            },
        ])
    }
}

fn is_v3_style(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
            | (DexKind::UniswapV3, PoolVariant::UniswapV3)
            | (DexKind::PancakeSwap, PoolVariant::PancakeV3)
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

fn decode_v3_liquidity_amount(data: &str, is_mint: bool) -> Result<U256> {
    let clean = data.trim_start_matches("0x");
    let amount_start = if is_mint { 64 } else { 0 };
    let amount_end = amount_start + 64;
    if clean.len() < amount_end {
        anyhow::bail!("V3 Mint/Burn data too short");
    }
    U256::from_str_radix(&clean[amount_start..amount_end], 16).map_err(Into::into)
}

fn decode_i24_topic(topic: &serde_json::Value) -> Result<i32> {
    let value = topic
        .as_str()
        .context("V3 indexed tick topic is not a string")?
        .trim_start_matches("0x");
    decode_i24_word(value)
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
            factory_address: None,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: None,
            pool_key_fee_pips: None,
            hooks_address: None,
            stable: Some(false),
            reserve0: Some(U256::from(1u64)),
            reserve1: Some(U256::from(2u64)),
            balancer_model: None,
            balancer_weight0: None,
            balancer_weight1: None,
            balancer_scaling_factor0: None,
            balancer_scaling_factor1: None,
            balancer_token_rate0: None,
            balancer_token_rate1: None,
            balancer_swap_fee_percentage: None,
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
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
                "topics": [super::AERODROME_SYNC_TOPIC],
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
            factory_address: None,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 5,
            fee_pips: Some(500),
            pool_key_fee_pips: None,
            hooks_address: None,
            stable: None,
            reserve0: None,
            reserve1: None,
            balancer_model: None,
            balancer_weight0: None,
            balancer_weight1: None,
            balancer_scaling_factor0: None,
            balancer_scaling_factor1: None,
            balancer_token_rate0: None,
            balancer_token_rate1: None,
            balancer_swap_fee_percentage: None,
            sqrt_price_x96: Some(U256::from(1u64)),
            liquidity: Some(U256::from(2u64)),
            tick: Some(3),
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
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
            factory_address: None,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: Some(3_000),
            pool_key_fee_pips: None,
            hooks_address: None,
            stable: None,
            reserve0: None,
            reserve1: None,
            balancer_model: None,
            balancer_weight0: None,
            balancer_weight1: None,
            balancer_scaling_factor0: None,
            balancer_scaling_factor1: None,
            balancer_token_rate0: None,
            balancer_token_rate1: None,
            balancer_swap_fee_percentage: None,
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
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

    #[test]
    fn applies_pancake_v3_swap_state_with_protocol_fee_words() {
        let pool = address!("7777777777777777777777777777777777777777");
        let mut state = v3_state(pool, Some(3), Some(U256::from(2u64)));
        state.dex = DexKind::PancakeSwap;
        state.variant = PoolVariant::PancakeV3;
        let event = DexEvent {
            block_number: 15,
            tx_hash: "0xpancake".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::PancakeSwap,
            event_type: "Swap".into(),
            raw_data_json: json!({
                "topics": [super::PANCAKE_V3_SWAP_TOPIC],
                "data": concat!(
                    "0x",
                    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
                    "0000000000000000000000000000000000000000000000000000000000000001",
                    "000000000000000000000000000000000000000000000000000000000000007b",
                    "00000000000000000000000000000000000000000000000000000000000001c8",
                    "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcf1da",
                    "0000000000000000000000000000000000000000000000000000000000000000",
                    "0000000000000000000000000000000000000000000000000000000000000000"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(changed);
        assert_eq!(state.sqrt_price_x96, Some(U256::from(123u64)));
        assert_eq!(state.liquidity, Some(U256::from(456u64)));
        assert_eq!(state.tick, Some(-200_230));
        assert_eq!(state.block_number, 15);
    }

    #[test]
    fn ignores_v3_mint_for_active_liquidity_local_state() {
        let pool = address!("4444444444444444444444444444444444444444");
        let mut state = v3_state(pool, Some(42), Some(U256::from(100u64)));
        let event = DexEvent {
            block_number: 12,
            tx_hash: "0xjkl".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::UniswapV3,
            event_type: "Mint".into(),
            raw_data_json: json!({
                "topics": [
                    super::V3_MINT_TOPIC,
                    "0x0000000000000000000000001111111111111111111111111111111111111111",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "0x0000000000000000000000000000000000000000000000000000000000000064"
                ],
                "data": concat!(
                    "0x",
                    "0000000000000000000000002222222222222222222222222222222222222222",
                    "000000000000000000000000000000000000000000000000000000000000000a",
                    "0000000000000000000000000000000000000000000000000000000000000001",
                    "0000000000000000000000000000000000000000000000000000000000000002"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(!changed);
        assert_eq!(state.liquidity, Some(U256::from(100u64)));
        assert_eq!(state.block_number, 1);
    }

    #[test]
    fn ignores_v3_burn_for_active_liquidity_local_state() {
        let pool = address!("5555555555555555555555555555555555555555");
        let mut state = v3_state(pool, Some(42), Some(U256::from(100u64)));
        let event = DexEvent {
            block_number: 13,
            tx_hash: "0xmno".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::UniswapV3,
            event_type: "Burn".into(),
            raw_data_json: json!({
                "topics": [
                    super::V3_BURN_TOPIC,
                    "0x0000000000000000000000001111111111111111111111111111111111111111",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "0x0000000000000000000000000000000000000000000000000000000000000064"
                ],
                "data": concat!(
                    "0x",
                    "0000000000000000000000000000000000000000000000000000000000000007",
                    "0000000000000000000000000000000000000000000000000000000000000001",
                    "0000000000000000000000000000000000000000000000000000000000000002"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(!changed);
        assert_eq!(state.liquidity, Some(U256::from(100u64)));
        assert_eq!(state.block_number, 1);
    }

    #[test]
    fn ignores_v3_liquidity_change_outside_active_range() {
        let pool = address!("6666666666666666666666666666666666666666");
        let mut state = v3_state(pool, Some(150), Some(U256::from(100u64)));
        let event = DexEvent {
            block_number: 14,
            tx_hash: "0xpqr".into(),
            log_index: 0,
            pool_address: pool,
            dex: DexKind::UniswapV3,
            event_type: "Mint".into(),
            raw_data_json: json!({
                "topics": [
                    super::V3_MINT_TOPIC,
                    "0x0000000000000000000000001111111111111111111111111111111111111111",
                    "0x0000000000000000000000000000000000000000000000000000000000000000",
                    "0x0000000000000000000000000000000000000000000000000000000000000064"
                ],
                "data": concat!(
                    "0x",
                    "0000000000000000000000002222222222222222222222222222222222222222",
                    "000000000000000000000000000000000000000000000000000000000000000a"
                )
            }),
        };

        let changed = apply_event_to_pool_state(&mut state, &event).unwrap();

        assert!(!changed);
        assert_eq!(state.liquidity, Some(U256::from(100u64)));
        assert_eq!(state.block_number, 1);
    }

    fn v3_state(
        pool: alloy_primitives::Address,
        tick: Option<i32>,
        liquidity: Option<U256>,
    ) -> PoolState {
        PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            factory_address: None,
            token0: address!("4200000000000000000000000000000000000006"),
            token1: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 5,
            fee_pips: Some(500),
            pool_key_fee_pips: None,
            hooks_address: None,
            stable: None,
            reserve0: None,
            reserve1: None,
            balancer_model: None,
            balancer_weight0: None,
            balancer_weight1: None,
            balancer_scaling_factor0: None,
            balancer_scaling_factor1: None,
            balancer_token_rate0: None,
            balancer_token_rate1: None,
            balancer_swap_fee_percentage: None,
            sqrt_price_x96: Some(U256::from(1u64)),
            liquidity,
            tick,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
            updated_at: Utc::now(),
        }
    }
}
