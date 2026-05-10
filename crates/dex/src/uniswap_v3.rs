use alloy_primitives::{Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{PoolState, QuoteResult};

use crate::quoter::DexQuoter;

const Q96_BITS: usize = 96;

#[derive(Debug, Clone, Default)]
pub struct UniswapV3CurrentTickQuoter;

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

        let amount_in_less_fee = apply_fee(amount_in, pool_state.fee_bps)?;
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

fn apply_fee(amount_in: U256, fee_bps: u32) -> Result<U256> {
    let fee_denominator = U256::from(10_000u64);
    let fee_numerator = fee_denominator
        .checked_sub(U256::from(fee_bps))
        .ok_or_else(|| ArbBotError::Quote("invalid fee".into()))?;
    amount_in
        .checked_mul(fee_numerator)
        .and_then(|value| value.checked_div(fee_denominator))
        .ok_or_else(|| ArbBotError::Quote("fee calculation overflow".into()))
}

fn quote_token0_for_token1(amount_in: U256, sqrt_price_x96: U256, liquidity: U256) -> Result<U256> {
    if amount_in.is_zero() {
        return Ok(U256::ZERO);
    }
    let q96 = q96();
    let scaled_amount = amount_in
        .checked_mul(sqrt_price_x96)
        .and_then(|value| value.checked_div(q96))
        .ok_or_else(|| ArbBotError::Quote("scaled amount overflow".into()))?;
    let denominator = liquidity
        .checked_add(scaled_amount)
        .ok_or_else(|| ArbBotError::Quote("sqrt denominator overflow".into()))?;
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("zero sqrt denominator".into()));
    }
    let next_sqrt_price = liquidity
        .checked_mul(sqrt_price_x96)
        .and_then(|value| value.checked_div(denominator))
        .ok_or_else(|| ArbBotError::Quote("next sqrt overflow".into()))?;
    let sqrt_delta = sqrt_price_x96.saturating_sub(next_sqrt_price);
    liquidity
        .checked_mul(sqrt_delta)
        .and_then(|value| value.checked_div(q96))
        .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))
}

fn quote_token1_for_token0(amount_in: U256, sqrt_price_x96: U256, liquidity: U256) -> Result<U256> {
    if amount_in.is_zero() {
        return Ok(U256::ZERO);
    }
    let q96 = q96();
    let sqrt_delta = amount_in
        .checked_mul(q96)
        .and_then(|value| value.checked_div(liquidity))
        .ok_or_else(|| ArbBotError::Quote("sqrt delta overflow".into()))?;
    let next_sqrt_price = sqrt_price_x96
        .checked_add(sqrt_delta)
        .ok_or_else(|| ArbBotError::Quote("next sqrt overflow".into()))?;
    let numerator = liquidity
        .checked_mul(sqrt_delta)
        .and_then(|value| value.checked_mul(q96))
        .ok_or_else(|| ArbBotError::Quote("amount0 numerator overflow".into()))?;
    let denominator = next_sqrt_price
        .checked_mul(sqrt_price_x96)
        .ok_or_else(|| ArbBotError::Quote("amount0 denominator overflow".into()))?;
    if denominator.is_zero() {
        return Err(ArbBotError::Quote("zero amount0 denominator".into()));
    }
    numerator
        .checked_div(denominator)
        .ok_or_else(|| ArbBotError::Quote("amount_out overflow".into()))
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

    use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};

    use crate::quoter::DexQuoter;

    use super::{spot_quote_exact_in, UniswapV3CurrentTickQuoter};

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
            token0,
            token1,
            fee_bps: 30,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(1u64) << 96),
            liquidity: Some(U256::from(1_000_000_000_000u64)),
            tick: Some(0),
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
}
