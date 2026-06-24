use std::str::FromStr;

use alloy_primitives::{Address, U256};

use base_arb_common::types::PoolState;

const BALANCER_WEIGHTED_MAX_IN_RATIO: f64 = 0.30;
const BALANCER_WEIGHTED_LOCAL_QUOTE_HAIRCUT_BPS: f64 = 5.0;
const BALANCER_FIXED_POINT_ONE: f64 = 1_000_000_000_000_000_000.0;

pub fn quote_weighted_exact_in(
    state: &PoolState,
    token_in: Address,
    token_out: Address,
    amount_in_raw: U256,
) -> Result<U256, String> {
    if amount_in_raw.is_zero() {
        return Err("Balancer V3 amount_in is zero".into());
    }
    let token0_in = token_in == state.token0 && token_out == state.token1;
    let token1_in = token_in == state.token1 && token_out == state.token0;
    if !token0_in && !token1_in {
        return Err("Balancer V3 token pair mismatch".into());
    }

    let reserve0 = state
        .reserve0
        .ok_or_else(|| "Balancer V3 missing token0 scaled balance".to_string())?;
    let reserve1 = state
        .reserve1
        .ok_or_else(|| "Balancer V3 missing token1 scaled balance".to_string())?;
    let weight0 = state
        .balancer_weight0
        .ok_or_else(|| "Balancer V3 missing token0 weight".to_string())?;
    let weight1 = state
        .balancer_weight1
        .ok_or_else(|| "Balancer V3 missing token1 weight".to_string())?;
    let scaling0 = state
        .balancer_scaling_factor0
        .ok_or_else(|| "Balancer V3 missing token0 scaling factor".to_string())?;
    let scaling1 = state
        .balancer_scaling_factor1
        .ok_or_else(|| "Balancer V3 missing token1 scaling factor".to_string())?;
    let rate0 = state
        .balancer_token_rate0
        .ok_or_else(|| "Balancer V3 missing token0 rate".to_string())?;
    let rate1 = state
        .balancer_token_rate1
        .ok_or_else(|| "Balancer V3 missing token1 rate".to_string())?;

    let (
        balance_in,
        weight_in,
        balance_out,
        weight_out,
        scaling_in,
        scaling_out,
        rate_in,
        rate_out,
    ) = if token0_in {
        (
            reserve0, weight0, reserve1, weight1, scaling0, scaling1, rate0, rate1,
        )
    } else {
        (
            reserve1, weight1, reserve0, weight0, scaling1, scaling0, rate1, rate0,
        )
    };

    let balance_in = u256_to_f64(balance_in)?;
    let balance_out = u256_to_f64(balance_out)?;
    let weight_in = u256_to_f64(weight_in)?;
    let weight_out = u256_to_f64(weight_out)?;
    let scaling_in = u256_to_f64(scaling_in)?;
    let scaling_out = u256_to_f64(scaling_out)?;
    let rate_in = u256_to_f64(rate_in)?;
    let rate_out = u256_to_f64(rate_out)?;
    let amount_in = u256_to_f64(amount_in_raw)?;
    if balance_in <= 0.0 || balance_out <= 0.0 || weight_in <= 0.0 || weight_out <= 0.0 {
        return Err("Balancer V3 invalid zero balance or weight".into());
    }
    let amount_in_scaled = amount_in * scaling_in * rate_in / BALANCER_FIXED_POINT_ONE;
    if !amount_in_scaled.is_finite() || amount_in_scaled <= 0.0 {
        return Err("Balancer V3 invalid scaled input amount".into());
    }
    if amount_in_scaled > balance_in * BALANCER_WEIGHTED_MAX_IN_RATIO {
        return Err("Balancer V3 input exceeds weighted max in ratio".into());
    }

    let fee_fraction = state
        .balancer_swap_fee_percentage
        .map(u256_to_f64)
        .transpose()?
        .unwrap_or_else(|| state.fee_bps as f64 * 1e14)
        / BALANCER_FIXED_POINT_ONE;
    let amount_after_fee = amount_in_scaled * (1.0 - fee_fraction.clamp(0.0, 0.99));
    let base = balance_in / (balance_in + amount_after_fee);
    let exponent = weight_in / weight_out;
    let amount_out_scaled = balance_out * (1.0 - base.powf(exponent));
    let amount_out_raw = amount_out_scaled * BALANCER_FIXED_POINT_ONE / rate_out / scaling_out;
    let amount_out_raw =
        amount_out_raw * (1.0 - BALANCER_WEIGHTED_LOCAL_QUOTE_HAIRCUT_BPS / 10_000.0);
    f64_to_u256_floor(amount_out_raw)
}

fn u256_to_f64(value: U256) -> Result<f64, String> {
    let parsed = value
        .to_string()
        .parse::<f64>()
        .map_err(|err| format!("U256 to f64 conversion failed: {err}"))?;
    if !parsed.is_finite() {
        return Err("U256 to f64 conversion produced non-finite value".into());
    }
    Ok(parsed)
}

fn f64_to_u256_floor(value: f64) -> Result<U256, String> {
    if !value.is_finite() || value <= 0.0 {
        return Err("Balancer V3 quote produced non-positive output".into());
    }
    let floored = value.floor();
    U256::from_str(&format!("{floored:.0}"))
        .map_err(|err| format!("f64 to U256 conversion failed: {err}"))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};
    use chrono::Utc;

    use super::quote_weighted_exact_in;

    #[test]
    fn quotes_two_token_weighted_pool() {
        let usdc = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let weth = address!("4200000000000000000000000000000000000006");
        let one_18 = U256::from(1_000_000_000_000_000_000u128);
        let state = PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: address!("9999999999999999999999999999999999999999"),
            },
            dex: DexKind::Balancer,
            variant: PoolVariant::BalancerV3,
            factory_address: None,
            token0: usdc,
            token1: weth,
            token0_decimals: Some(6),
            token1_decimals: Some(18),
            fee_bps: 10,
            fee_pips: None,
            pool_key_fee_pips: None,
            hooks_address: None,
            stable: None,
            reserve0: Some(U256::from(100u64) * one_18),
            reserve1: Some(one_18),
            balancer_model: Some("weighted".to_string()),
            balancer_weight0: Some(one_18 / U256::from(2u64)),
            balancer_weight1: Some(one_18 / U256::from(2u64)),
            balancer_scaling_factor0: Some(U256::from(1_000_000_000_000u64)),
            balancer_scaling_factor1: Some(U256::from(1u64)),
            balancer_token_rate0: Some(one_18),
            balancer_token_rate1: Some(one_18),
            balancer_swap_fee_percentage: Some(U256::from(1_000_000_000_000_000u64)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
            updated_at: Utc::now(),
        };

        let out = quote_weighted_exact_in(&state, usdc, weth, U256::from(5_000_000u64))
            .expect("local weighted quote should work");

        assert!(out > U256::from(40_000_000_000_000_000u64));
        assert!(out < U256::from(50_000_000_000_000_000u64));
    }
}
