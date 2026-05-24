use alloy_primitives::{aliases::U512, Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{PoolState, PoolVariant, QuoteResult, TickState};

use crate::quoter::DexQuoter;

const Q96_BITS: usize = 96;
const FEE_DENOMINATOR: u32 = 1_000_000;

#[derive(Debug, Clone, Default)]
pub struct UniswapV3CurrentTickQuoter;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct V3QuoteDiagnostics {
    pub ticks_used: u32,
    pub crossed_ticks: u32,
    pub tick_range_exhausted: bool,
}

#[async_trait]
impl DexQuoter for UniswapV3CurrentTickQuoter {
    async fn quote_exact_in(
        &self,
        pool_state: &PoolState,
        token_in: Address,
        amount_in: U256,
    ) -> Result<QuoteResult> {
        if token_in != pool_state.token0 && token_in != pool_state.token1 {
            return Err(ArbBotError::Quote("token_in not in pool".into()));
        }

        let sqrt_price_x96 = pool_state
            .sqrt_price_x96
            .ok_or_else(|| ArbBotError::Quote("missing sqrt_price_x96".into()))?;
        let liquidity = pool_state
            .liquidity
            .ok_or_else(|| ArbBotError::Quote("missing liquidity".into()))?;
        if sqrt_price_x96.is_zero() || liquidity.is_zero() {
            return Err(ArbBotError::Quote("empty V3 state".into()));
        }

        let fee_pips = v3_fee_pips(pool_state)?;
        let amount_in_less_fee = mul_div(
            amount_in,
            U256::from(FEE_DENOMINATOR.saturating_sub(fee_pips)),
            U256::from(FEE_DENOMINATOR),
        )?;
        let amount_out = if token_in == pool_state.token0 {
            quote_token0_for_token1(amount_in_less_fee, sqrt_price_x96, liquidity)?
        } else {
            quote_token1_for_token0(amount_in_less_fee, sqrt_price_x96, liquidity)?
        };

        Ok(QuoteResult {
            amount_in,
            amount_out,
            gas_estimate: None,
        })
    }
}

pub fn spot_quote_exact_in(
    pool_state: &PoolState,
    token_in: Address,
    amount_in: U256,
) -> Result<U256> {
    let sqrt_price_x96 = pool_state
        .sqrt_price_x96
        .ok_or_else(|| ArbBotError::Quote("missing sqrt_price_x96".into()))?;
    if sqrt_price_x96.is_zero() {
        return Err(ArbBotError::Quote("empty sqrt_price_x96".into()));
    }
    if token_in == pool_state.token0 {
        let price_x192 = sqrt_price_x96
            .checked_mul(sqrt_price_x96)
            .ok_or_else(|| ArbBotError::Quote("price overflow".into()))?;
        amount_in
            .checked_mul(price_x192)
            .and_then(|value| value.checked_div(q192()))
            .ok_or_else(|| ArbBotError::Quote("spot quote overflow".into()))
    } else if token_in == pool_state.token1 {
        let price_x192 = sqrt_price_x96
            .checked_mul(sqrt_price_x96)
            .ok_or_else(|| ArbBotError::Quote("price overflow".into()))?;
        amount_in
            .checked_mul(q192())
            .and_then(|value| value.checked_div(price_x192))
            .ok_or_else(|| ArbBotError::Quote("spot quote overflow".into()))
    } else {
        Err(ArbBotError::Quote("token_in not in pool".into()))
    }
}

pub fn quote_exact_in_with_ticks(
    pool_state: &PoolState,
    initialized_ticks: &[TickState],
    token_in: Address,
    amount_in: U256,
) -> Result<QuoteResult> {
    let (quote, _) =
        quote_exact_in_with_ticks_diagnostics(pool_state, initialized_ticks, token_in, amount_in)?;
    Ok(quote)
}

pub fn quote_exact_in_with_ticks_diagnostics(
    pool_state: &PoolState,
    initialized_ticks: &[TickState],
    token_in: Address,
    amount_in: U256,
) -> Result<(QuoteResult, V3QuoteDiagnostics)> {
    if initialized_ticks.is_empty() {
        return Err(ArbBotError::Quote("missing initialized ticks".into()));
    }
    if token_in != pool_state.token0 && token_in != pool_state.token1 {
        return Err(ArbBotError::Quote("token_in not in pool".into()));
    }
    let sqrt_price_x96 = pool_state
        .sqrt_price_x96
        .ok_or_else(|| ArbBotError::Quote("missing sqrt_price_x96".into()))?;
    let liquidity = pool_state
        .liquidity
        .ok_or_else(|| ArbBotError::Quote("missing liquidity".into()))?;
    let current_tick = pool_state
        .tick
        .ok_or_else(|| ArbBotError::Quote("missing tick".into()))?;
    let fee_pips = v3_fee_pips(pool_state)?;
    let (amount_out, diagnostics) = if token_in == pool_state.token0 {
        simulate_zero_for_one(
            amount_in,
            sqrt_price_x96,
            liquidity,
            current_tick,
            initialized_ticks,
            fee_pips,
        )?
    } else {
        simulate_one_for_zero(
            amount_in,
            sqrt_price_x96,
            liquidity,
            current_tick,
            initialized_ticks,
            fee_pips,
        )?
    };

    Ok((
        QuoteResult {
            amount_in,
            amount_out,
            gas_estimate: None,
        },
        diagnostics,
    ))
}

fn simulate_zero_for_one(
    mut amount_remaining: U256,
    mut sqrt_price: U256,
    mut liquidity: U256,
    current_tick: i32,
    initialized_ticks: &[TickState],
    fee_pips: u32,
) -> Result<(U256, V3QuoteDiagnostics)> {
    let mut amount_out = U256::ZERO;
    let mut diagnostics = V3QuoteDiagnostics::default();
    let mut ticks = initialized_ticks
        .iter()
        .filter(|tick| tick.tick < current_tick)
        .collect::<Vec<_>>();
    ticks.sort_by_key(|tick| std::cmp::Reverse(tick.tick));

    for tick in ticks {
        if amount_remaining.is_zero() || liquidity.is_zero() {
            break;
        }
        diagnostics.ticks_used += 1;
        let target_sqrt = sqrt_ratio_at_tick(tick.tick)?;
        let step = compute_swap_step(
            sqrt_price,
            target_sqrt,
            liquidity,
            amount_remaining,
            fee_pips,
            true,
        )?;
        let step_consumed = step
            .amount_in
            .checked_add(step.fee_amount)
            .ok_or_else(|| ArbBotError::Quote("step amount overflow".into()))?;
        amount_remaining = amount_remaining.saturating_sub(step_consumed);
        amount_out = amount_out
            .checked_add(step.amount_out)
            .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))?;
        sqrt_price = step.sqrt_next;
        if sqrt_price == target_sqrt {
            diagnostics.crossed_ticks += 1;
            liquidity = apply_liquidity_delta(liquidity, -tick.liquidity_net)?;
        } else {
            return Ok((amount_out, diagnostics));
        }
    }

    if !amount_remaining.is_zero() && !liquidity.is_zero() {
        diagnostics.tick_range_exhausted = true;
        let step = compute_swap_step(
            sqrt_price,
            U256::from(1u64),
            liquidity,
            amount_remaining,
            fee_pips,
            true,
        )?;
        amount_out = amount_out
            .checked_add(step.amount_out)
            .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))?;
    }
    Ok((amount_out, diagnostics))
}

fn simulate_one_for_zero(
    mut amount_remaining: U256,
    mut sqrt_price: U256,
    mut liquidity: U256,
    current_tick: i32,
    initialized_ticks: &[TickState],
    fee_pips: u32,
) -> Result<(U256, V3QuoteDiagnostics)> {
    let mut amount_out = U256::ZERO;
    let mut diagnostics = V3QuoteDiagnostics::default();
    let mut ticks = initialized_ticks
        .iter()
        .filter(|tick| tick.tick > current_tick)
        .collect::<Vec<_>>();
    ticks.sort_by_key(|tick| tick.tick);

    for tick in ticks {
        if amount_remaining.is_zero() || liquidity.is_zero() {
            break;
        }
        diagnostics.ticks_used += 1;
        let target_sqrt = sqrt_ratio_at_tick(tick.tick)?;
        let step = compute_swap_step(
            sqrt_price,
            target_sqrt,
            liquidity,
            amount_remaining,
            fee_pips,
            false,
        )?;
        let step_consumed = step
            .amount_in
            .checked_add(step.fee_amount)
            .ok_or_else(|| ArbBotError::Quote("step amount overflow".into()))?;
        amount_remaining = amount_remaining.saturating_sub(step_consumed);
        amount_out = amount_out
            .checked_add(step.amount_out)
            .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))?;
        sqrt_price = step.sqrt_next;
        if sqrt_price == target_sqrt {
            diagnostics.crossed_ticks += 1;
            liquidity = apply_liquidity_delta(liquidity, tick.liquidity_net)?;
        } else {
            return Ok((amount_out, diagnostics));
        }
    }

    if !amount_remaining.is_zero() && !liquidity.is_zero() {
        diagnostics.tick_range_exhausted = true;
        let step = compute_swap_step(
            sqrt_price,
            U256::MAX,
            liquidity,
            amount_remaining,
            fee_pips,
            false,
        )?;
        amount_out = amount_out
            .checked_add(step.amount_out)
            .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))?;
    }
    Ok((amount_out, diagnostics))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SwapStepResult {
    sqrt_next: U256,
    amount_in: U256,
    amount_out: U256,
    fee_amount: U256,
}

fn compute_swap_step(
    sqrt_current: U256,
    sqrt_target: U256,
    liquidity: U256,
    amount_remaining: U256,
    fee_pips: u32,
    zero_for_one: bool,
) -> Result<SwapStepResult> {
    if amount_remaining.is_zero() || liquidity.is_zero() {
        return Ok(SwapStepResult {
            sqrt_next: sqrt_current,
            amount_in: U256::ZERO,
            amount_out: U256::ZERO,
            fee_amount: U256::ZERO,
        });
    }

    let amount_remaining_less_fee = mul_div(
        amount_remaining,
        U256::from(FEE_DENOMINATOR.saturating_sub(fee_pips)),
        U256::from(FEE_DENOMINATOR),
    )?;
    let amount_in_to_target = if zero_for_one {
        amount0_delta(liquidity, sqrt_target, sqrt_current, true)?
    } else {
        amount1_delta(liquidity, sqrt_current, sqrt_target, true)?
    };
    let reached_target = amount_remaining_less_fee >= amount_in_to_target;
    let sqrt_next = if reached_target {
        sqrt_target
    } else if zero_for_one {
        next_sqrt_price_from_amount0_in(sqrt_current, liquidity, amount_remaining_less_fee)?
    } else {
        next_sqrt_price_from_amount1_in(sqrt_current, liquidity, amount_remaining_less_fee)?
    };

    let amount_in = if zero_for_one {
        amount0_delta(liquidity, sqrt_next, sqrt_current, true)?
    } else {
        amount1_delta(liquidity, sqrt_current, sqrt_next, true)?
    };
    let amount_out = if zero_for_one {
        amount1_delta(liquidity, sqrt_next, sqrt_current, false)?
    } else {
        amount0_delta(liquidity, sqrt_current, sqrt_next, false)?
    };
    let fee_amount = if reached_target {
        mul_div_rounding_up(
            amount_in,
            U256::from(fee_pips),
            U256::from(FEE_DENOMINATOR.saturating_sub(fee_pips)),
        )?
    } else {
        amount_remaining.saturating_sub(amount_in)
    };

    Ok(SwapStepResult {
        sqrt_next,
        amount_in,
        amount_out,
        fee_amount,
    })
}

fn v3_fee_pips(pool_state: &PoolState) -> Result<u32> {
    if pool_state.variant == PoolVariant::AerodromeSlipstream && pool_state.fee_pips.is_none() {
        return Err(ArbBotError::Quote(
            "missing Aerodrome Slipstream fee_pips".into(),
        ));
    }
    let fee_pips = pool_state
        .fee_pips
        .unwrap_or_else(|| pool_state.fee_bps.saturating_mul(100));
    if fee_pips >= FEE_DENOMINATOR {
        return Err(ArbBotError::Quote("invalid V3 fee".into()));
    }
    Ok(fee_pips)
}

fn quote_token0_for_token1(amount_in: U256, sqrt_price_x96: U256, liquidity: U256) -> Result<U256> {
    if amount_in.is_zero() {
        return Ok(U256::ZERO);
    }
    let next_sqrt_price = next_sqrt_price_from_amount0_in(sqrt_price_x96, liquidity, amount_in)?;
    amount1_delta(liquidity, next_sqrt_price, sqrt_price_x96, false)
}

fn quote_token1_for_token0(amount_in: U256, sqrt_price_x96: U256, liquidity: U256) -> Result<U256> {
    if amount_in.is_zero() {
        return Ok(U256::ZERO);
    }
    let next_sqrt_price = next_sqrt_price_from_amount1_in(sqrt_price_x96, liquidity, amount_in)?;
    amount0_delta(liquidity, sqrt_price_x96, next_sqrt_price, false)
}

fn amount0_delta(liquidity: U256, sqrt_a: U256, sqrt_b: U256, round_up: bool) -> Result<U256> {
    let (lower, upper) = ordered(sqrt_a, sqrt_b);
    let diff = upper.saturating_sub(lower);
    let numerator1 = liquidity
        .checked_shl(Q96_BITS)
        .ok_or_else(|| ArbBotError::Quote("amount0 numerator overflow".into()))?;
    if round_up {
        let value = mul_div_rounding_up(numerator1, diff, upper)?;
        div_round(value, lower, true)
    } else {
        let value = mul_div(numerator1, diff, upper)?;
        value
            .checked_div(lower)
            .ok_or_else(|| ArbBotError::Quote("amount0 denominator overflow".into()))
    }
}

fn amount1_delta(liquidity: U256, sqrt_a: U256, sqrt_b: U256, round_up: bool) -> Result<U256> {
    let (lower, upper) = ordered(sqrt_a, sqrt_b);
    let diff = upper.saturating_sub(lower);
    if round_up {
        mul_div_rounding_up(liquidity, diff, q96())
    } else {
        mul_div(liquidity, diff, q96())
    }
}

fn next_sqrt_price_from_amount0_in(
    sqrt_price: U256,
    liquidity: U256,
    amount_in: U256,
) -> Result<U256> {
    let numerator1 = liquidity
        .checked_shl(Q96_BITS)
        .ok_or_else(|| ArbBotError::Quote("next sqrt numerator overflow".into()))?;
    let denominator = U512::from(numerator1)
        .checked_add(U512::from(amount_in) * U512::from(sqrt_price))
        .ok_or_else(|| ArbBotError::Quote("next sqrt denominator overflow".into()))?;
    mul_div_rounding_up_u512(numerator1, sqrt_price, denominator)
}

fn next_sqrt_price_from_amount1_in(
    sqrt_price: U256,
    liquidity: U256,
    amount_in: U256,
) -> Result<U256> {
    let delta = mul_div(amount_in, q96(), liquidity)?;
    sqrt_price
        .checked_add(delta)
        .ok_or_else(|| ArbBotError::Quote("next sqrt overflow".into()))
}

fn apply_liquidity_delta(liquidity: U256, delta: i128) -> Result<U256> {
    if delta >= 0 {
        liquidity
            .checked_add(U256::from(delta as u128))
            .ok_or_else(|| ArbBotError::Quote("liquidity overflow".into()))
    } else {
        Ok(liquidity.saturating_sub(U256::from((-delta) as u128)))
    }
}

fn ordered(a: U256, b: U256) -> (U256, U256) {
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

fn div_round(numerator: U256, denominator: U256, round_up: bool) -> Result<U256> {
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("division by zero".into()));
    }
    let quotient = numerator / denominator;
    if round_up && numerator % denominator != U256::ZERO {
        quotient
            .checked_add(U256::from(1u64))
            .ok_or_else(|| ArbBotError::Quote("rounded division overflow".into()))
    } else {
        Ok(quotient)
    }
}

fn mul_div(a: U256, b: U256, denominator: U256) -> Result<U256> {
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("division by zero".into()));
    }
    u512_to_u256((U512::from(a) * U512::from(b)) / U512::from(denominator))
}

fn mul_div_rounding_up(a: U256, b: U256, denominator: U256) -> Result<U256> {
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("division by zero".into()));
    }
    let numerator = U512::from(a) * U512::from(b);
    let denominator = U512::from(denominator);
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let value = u512_to_u256(quotient)?;
    if remainder.is_zero() {
        Ok(value)
    } else {
        value
            .checked_add(U256::from(1u64))
            .ok_or_else(|| ArbBotError::Quote("mulDiv rounding overflow".into()))
    }
}

fn mul_div_rounding_up_u512(a: U256, b: U256, denominator: U512) -> Result<U256> {
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("division by zero".into()));
    }
    let numerator = U512::from(a) * U512::from(b);
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let value = u512_to_u256(quotient)?;
    if remainder.is_zero() {
        Ok(value)
    } else {
        value
            .checked_add(U256::from(1u64))
            .ok_or_else(|| ArbBotError::Quote("mulDiv rounding overflow".into()))
    }
}

fn u512_to_u256(value: U512) -> Result<U256> {
    U256::checked_from_limbs_slice(value.as_limbs())
        .ok_or_else(|| ArbBotError::Quote("mulDiv overflow".into()))
}

fn sqrt_ratio_at_tick(tick: i32) -> Result<U256> {
    const MIN_TICK: i32 = -887_272;
    const MAX_TICK: i32 = 887_272;
    if !(MIN_TICK..=MAX_TICK).contains(&tick) {
        return Err(ArbBotError::Quote("tick out of range".into()));
    }
    let abs_tick = tick.unsigned_abs();
    let mut ratio = if (abs_tick & 0x1) != 0 {
        u256_hex("fffcb933bd6fad37aa2d162d1a594001")?
    } else {
        U256::from(1u64) << 128
    };

    for (mask, value) in [
        (0x2, "fff97272373d413259a46990580e213a"),
        (0x4, "fff2e50f5f656932ef12357cf3c7fdcc"),
        (0x8, "ffe5caca7e10e4e61c3624eaa0941cd0"),
        (0x10, "ffcb9843d60f6159c9db58835c926644"),
        (0x20, "ff973b41fa98c081472e6896dfb254c0"),
        (0x40, "ff2ea16466c96a3843ec78b326b52861"),
        (0x80, "fe5dee046a99a2a811c461f1969c3053"),
        (0x100, "fcbe86c7900a88aedcffc83b479aa3a4"),
        (0x200, "f987a7253ac413176f2b074cf7815e54"),
        (0x400, "f3392b0822b70005940c7a398e4b70f3"),
        (0x800, "e7159475a2c29b7443b29c7fa6e889d9"),
        (0x1000, "d097f3bdfd2022b8845ad8f792aa5825"),
        (0x2000, "a9f746462d870fdf8a65dc1f90e061e5"),
        (0x4000, "70d869a156d2a1b890bb3df62baf32f7"),
        (0x8000, "31be135f97d08fd981231505542fcfa6"),
        (0x10000, "9aa508b5b7a84e1c677de54f3e99bc9"),
        (0x20000, "5d6af8dedb81196699c329225ee604"),
        (0x40000, "2216e584f5fa1ea926041bedfe98"),
        (0x80000, "48a170391f7dc42444e8fa2"),
    ] {
        if (abs_tick & mask) != 0 {
            ratio = ratio
                .checked_mul(u256_hex(value)?)
                .map(|value| value >> 128)
                .ok_or_else(|| ArbBotError::Quote("tick ratio overflow".into()))?;
        }
    }

    if tick > 0 {
        ratio = U256::MAX / ratio;
    }
    let remainder_mask = (U256::from(1u64) << 32) - U256::from(1u64);
    let rounded = if ratio & remainder_mask == U256::ZERO {
        ratio >> 32
    } else {
        (ratio >> 32) + U256::from(1u64)
    };
    Ok(rounded)
}

fn u256_hex(value: &str) -> Result<U256> {
    U256::from_str_radix(value, 16).map_err(|_| ArbBotError::Quote("invalid hex constant".into()))
}

fn q96() -> U256 {
    U256::from(1u64) << Q96_BITS
}

fn q192() -> U256 {
    q96() * q96()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::Utc;

    use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant, TickState};

    use crate::quoter::DexQuoter;

    use super::{
        quote_exact_in_with_ticks_diagnostics, spot_quote_exact_in, UniswapV3CurrentTickQuoter,
    };

    #[tokio::test]
    async fn quotes_current_tick_in_both_directions() {
        let token0 = address!("4200000000000000000000000000000000000006");
        let token1 = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let state = PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: address!("1111111111111111111111111111111111111111"),
            },
            dex: DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            factory_address: None,
            token0,
            token1,
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: Some(3_000),
            stable: None,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(1u64) << 96),
            liquidity: Some(U256::from(1_000_000_000_000u64)),
            tick: Some(0),
            tick_spacing: None,
            block_number: 1,
            updated_at: Utc::now(),
        };
        let quoter = UniswapV3CurrentTickQuoter;

        let zero_for_one = quoter
            .quote_exact_in(&state, token0, U256::from(1_000u64))
            .await
            .unwrap();
        let one_for_zero = quoter
            .quote_exact_in(&state, token1, U256::from(1_000u64))
            .await
            .unwrap();

        assert!(zero_for_one.amount_out > U256::ZERO);
        assert!(one_for_zero.amount_out > U256::ZERO);
        assert!(spot_quote_exact_in(&state, token0, U256::from(1_000u64)).unwrap() > U256::ZERO);
    }

    #[test]
    fn quote_diagnostics_mark_exhausted_tick_range() {
        let token0 = address!("4200000000000000000000000000000000000006");
        let token1 = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let pool_id = PoolId {
            chain_id: 8453,
            address: address!("1111111111111111111111111111111111111111"),
        };
        let state = PoolState {
            pool_id: pool_id.clone(),
            dex: DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            factory_address: None,
            token0,
            token1,
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: Some(3_000),
            stable: None,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(1u64) << 96),
            liquidity: Some(U256::from(1_000_000_000_000u64)),
            tick: Some(0),
            tick_spacing: None,
            block_number: 1,
            updated_at: Utc::now(),
        };
        let ticks = vec![TickState {
            pool_id,
            tick: 100,
            liquidity_net: 1_000,
            liquidity_gross: U256::from(1_000u64),
            block_number: 1,
            updated_at: Utc::now(),
        }];

        let (_, diagnostics) =
            quote_exact_in_with_ticks_diagnostics(&state, &ticks, token0, U256::from(1_000u64))
                .unwrap();

        assert_eq!(diagnostics.ticks_used, 0);
        assert_eq!(diagnostics.crossed_ticks, 0);
        assert!(diagnostics.tick_range_exhausted);
    }
}
