use alloy_primitives::{Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{PoolState, QuoteResult};

use crate::quoter::DexQuoter;

#[derive(Debug, Clone, Default)]
pub struct AerodromeVolatileQuoter;

#[derive(Debug, Clone, Default)]
pub struct AerodromeStableQuoter;

impl AerodromeVolatileQuoter {
    pub fn quote_amount_out(
        reserve_in: U256,
        reserve_out: U256,
        amount_in: U256,
        fee_bps: u32,
    ) -> Result<U256> {
        if reserve_in.is_zero() || reserve_out.is_zero() {
            return Err(ArbBotError::Quote("empty reserve".into()));
        }
        if amount_in.is_zero() {
            return Ok(U256::ZERO);
        }

        let fee_denominator = U256::from(10_000u64);
        let fee_numerator = fee_denominator
            .checked_sub(U256::from(fee_bps))
            .ok_or_else(|| ArbBotError::Quote("invalid fee".into()))?;
        let amount_in_with_fee = amount_in
            .checked_mul(fee_numerator)
            .ok_or_else(|| ArbBotError::Quote("amount_in overflow".into()))?;
        let numerator = amount_in_with_fee
            .checked_mul(reserve_out)
            .ok_or_else(|| ArbBotError::Quote("numerator overflow".into()))?;
        let denominator = reserve_in
            .checked_mul(fee_denominator)
            .and_then(|v| v.checked_add(amount_in_with_fee))
            .ok_or_else(|| ArbBotError::Quote("denominator overflow".into()))?;

        Ok(numerator / denominator)
    }
}

impl AerodromeStableQuoter {
    const FEE_DENOMINATOR: u64 = 10_000;
    const ONE: u64 = 1_000_000_000_000_000_000;

    pub fn quote_amount_out(
        reserve_in: U256,
        reserve_out: U256,
        amount_in: U256,
        fee_bps: u32,
        decimals_in: u8,
        decimals_out: u8,
    ) -> Result<U256> {
        if reserve_in.is_zero() || reserve_out.is_zero() {
            return Err(ArbBotError::Quote("empty reserve".into()));
        }
        if amount_in.is_zero() {
            return Ok(U256::ZERO);
        }

        let amount_in_after_fee = amount_in
            .checked_mul(Self::fee_numerator(fee_bps)?)
            .and_then(|value| value.checked_div(U256::from(Self::FEE_DENOMINATOR)))
            .ok_or_else(|| ArbBotError::Quote("amount_in fee overflow".into()))?;
        let reserve_in_scaled = scale_to_1e18(reserve_in, decimals_in)?;
        let reserve_out_scaled = scale_to_1e18(reserve_out, decimals_out)?;
        let amount_in_scaled = scale_to_1e18(amount_in_after_fee, decimals_in)?;
        let xy = stable_k(reserve_in_scaled, reserve_out_scaled)?;
        let x0 = reserve_in_scaled
            .checked_add(amount_in_scaled)
            .ok_or_else(|| ArbBotError::Quote("stable x overflow".into()))?;
        let y = get_y(x0, xy, reserve_out_scaled)?;
        if y > reserve_out_scaled {
            return Err(ArbBotError::Quote("stable y overflow".into()));
        }
        let dy_scaled = reserve_out_scaled - y;

        unscale_from_1e18(dy_scaled, decimals_out)
    }

    fn fee_numerator(fee_bps: u32) -> Result<U256> {
        U256::from(Self::FEE_DENOMINATOR)
            .checked_sub(U256::from(fee_bps))
            .ok_or_else(|| ArbBotError::Quote("invalid fee".into()))
    }
}

fn one() -> U256 {
    U256::from(AerodromeStableQuoter::ONE)
}

fn pow10(decimals: u8) -> Result<U256> {
    let mut value = U256::from(1u64);
    for _ in 0..decimals {
        value = value
            .checked_mul(U256::from(10u64))
            .ok_or_else(|| ArbBotError::Quote("decimal scale overflow".into()))?;
    }
    Ok(value)
}

fn scale_to_1e18(value: U256, decimals: u8) -> Result<U256> {
    let scale = pow10(decimals)?;
    value
        .checked_mul(one())
        .and_then(|value| value.checked_div(scale))
        .ok_or_else(|| ArbBotError::Quote("stable scale overflow".into()))
}

fn unscale_from_1e18(value: U256, decimals: u8) -> Result<U256> {
    let scale = pow10(decimals)?;
    value
        .checked_mul(scale)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable unscale overflow".into()))
}

fn stable_k(x: U256, y: U256) -> Result<U256> {
    let a = x
        .checked_mul(y)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable k a overflow".into()))?;
    let x2 = x
        .checked_mul(x)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable k x2 overflow".into()))?;
    let y2 = y
        .checked_mul(y)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable k y2 overflow".into()))?;
    let b = x2
        .checked_add(y2)
        .ok_or_else(|| ArbBotError::Quote("stable k b overflow".into()))?;
    a.checked_mul(b)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable k overflow".into()))
}

fn stable_f(x0: U256, y: U256) -> Result<U256> {
    let a = x0
        .checked_mul(y)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable f a overflow".into()))?;
    let x2 = x0
        .checked_mul(x0)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable f x2 overflow".into()))?;
    let y2 = y
        .checked_mul(y)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable f y2 overflow".into()))?;
    let b = x2
        .checked_add(y2)
        .ok_or_else(|| ArbBotError::Quote("stable f b overflow".into()))?;
    a.checked_mul(b)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable f overflow".into()))
}

fn stable_d(x0: U256, y: U256) -> Result<U256> {
    let y2 = y
        .checked_mul(y)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable d y2 overflow".into()))?;
    let left = U256::from(3u64)
        .checked_mul(x0)
        .and_then(|value| value.checked_mul(y2))
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable d left overflow".into()))?;
    let x2 = x0
        .checked_mul(x0)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable d x2 overflow".into()))?;
    let right = x2
        .checked_mul(x0)
        .and_then(|value| value.checked_div(one()))
        .ok_or_else(|| ArbBotError::Quote("stable d right overflow".into()))?;
    left.checked_add(right)
        .ok_or_else(|| ArbBotError::Quote("stable d overflow".into()))
}

fn get_y(x0: U256, xy: U256, mut y: U256) -> Result<U256> {
    for _ in 0..255 {
        let k = stable_f(x0, y)?;
        let d = stable_d(x0, y)?;
        if d.is_zero() {
            return Err(ArbBotError::Quote("stable derivative is zero".into()));
        }

        if k < xy {
            let mut dy = xy
                .checked_sub(k)
                .and_then(|value| value.checked_mul(one()))
                .and_then(|value| value.checked_div(d))
                .ok_or_else(|| ArbBotError::Quote("stable y increment overflow".into()))?;
            if dy.is_zero() {
                if k == xy {
                    return Ok(y);
                }
                let y_plus_one = y
                    .checked_add(U256::from(1u64))
                    .ok_or_else(|| ArbBotError::Quote("stable y add overflow".into()))?;
                if stable_f(x0, y_plus_one)? > xy {
                    return Ok(y_plus_one);
                }
                dy = U256::from(1u64);
            }
            y = y
                .checked_add(dy)
                .ok_or_else(|| ArbBotError::Quote("stable y add overflow".into()))?;
        } else {
            let mut dy = k
                .checked_sub(xy)
                .and_then(|value| value.checked_mul(one()))
                .and_then(|value| value.checked_div(d))
                .ok_or_else(|| ArbBotError::Quote("stable y decrement overflow".into()))?;
            if dy.is_zero() {
                if k == xy {
                    return Ok(y);
                }
                if y.is_zero() || stable_f(x0, y - U256::from(1u64))? < xy {
                    return Ok(y);
                }
                dy = U256::from(1u64);
            }
            if dy > y {
                return Err(ArbBotError::Quote("stable y underflow".into()));
            }
            y -= dy;
        }
    }

    Err(ArbBotError::Quote("stable y did not converge".into()))
}

#[async_trait]
impl DexQuoter for AerodromeVolatileQuoter {
    async fn quote_exact_in(
        &self,
        pool_state: &PoolState,
        token_in: Address,
        amount_in: U256,
    ) -> Result<QuoteResult> {
        let reserve0 = pool_state
            .reserve0
            .ok_or_else(|| ArbBotError::Quote("missing reserve0".into()))?;
        let reserve1 = pool_state
            .reserve1
            .ok_or_else(|| ArbBotError::Quote("missing reserve1".into()))?;

        let amount_out = if token_in == pool_state.token0 {
            Self::quote_amount_out(reserve0, reserve1, amount_in, pool_state.fee_bps)?
        } else if token_in == pool_state.token1 {
            Self::quote_amount_out(reserve1, reserve0, amount_in, pool_state.fee_bps)?
        } else {
            return Err(ArbBotError::Quote("token_in not in pool".into()));
        };

        Ok(QuoteResult {
            amount_in,
            amount_out,
            gas_estimate: None,
        })
    }
}

#[async_trait]
impl DexQuoter for AerodromeStableQuoter {
    async fn quote_exact_in(
        &self,
        pool_state: &PoolState,
        token_in: Address,
        amount_in: U256,
    ) -> Result<QuoteResult> {
        let reserve0 = pool_state
            .reserve0
            .ok_or_else(|| ArbBotError::Quote("missing reserve0".into()))?;
        let reserve1 = pool_state
            .reserve1
            .ok_or_else(|| ArbBotError::Quote("missing reserve1".into()))?;
        let decimals0 = pool_state
            .token0_decimals
            .ok_or_else(|| ArbBotError::Quote("missing token0 decimals".into()))?;
        let decimals1 = pool_state
            .token1_decimals
            .ok_or_else(|| ArbBotError::Quote("missing token1 decimals".into()))?;

        let amount_out = if token_in == pool_state.token0 {
            Self::quote_amount_out(
                reserve0,
                reserve1,
                amount_in,
                pool_state.fee_bps,
                decimals0,
                decimals1,
            )?
        } else if token_in == pool_state.token1 {
            Self::quote_amount_out(
                reserve1,
                reserve0,
                amount_in,
                pool_state.fee_bps,
                decimals1,
                decimals0,
            )?
        } else {
            return Err(ArbBotError::Quote("token_in not in pool".into()));
        };

        Ok(QuoteResult {
            amount_in,
            amount_out,
            gas_estimate: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::{AerodromeStableQuoter, AerodromeVolatileQuoter};

    #[test]
    fn quotes_constant_product_with_fee() {
        let reserve_in = U256::from(1_000_000u64);
        let reserve_out = U256::from(500_000u64);
        let amount_in = U256::from(100_000u64);

        let out = AerodromeVolatileQuoter::quote_amount_out(reserve_in, reserve_out, amount_in, 30)
            .unwrap();

        assert_eq!(out, U256::from(45_330u64));
    }

    #[test]
    fn quotes_stable_pool_near_par_with_mixed_decimals() {
        let reserve_in = U256::from(1_000_000_000_000u64);
        let reserve_out = U256::from(1_000_000_000_000_000_000_000_000u128);
        let amount_in = U256::from(1_000_000u64);

        let out =
            AerodromeStableQuoter::quote_amount_out(reserve_in, reserve_out, amount_in, 30, 6, 18)
                .unwrap();

        assert!(out > U256::from(995_000_000_000_000_000u128));
        assert!(out < U256::from(998_000_000_000_000_000u128));
    }
}
