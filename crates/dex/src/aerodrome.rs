use alloy_primitives::{Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{PoolState, QuoteResult};

use crate::quoter::DexQuoter;

#[derive(Debug, Clone, Default)]
pub struct AerodromeVolatileQuoter;

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

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;

    use super::AerodromeVolatileQuoter;

    #[test]
    fn quotes_constant_product_with_fee() {
        let reserve_in = U256::from(1_000_000u64);
        let reserve_out = U256::from(500_000u64);
        let amount_in = U256::from(100_000u64);

        let out = AerodromeVolatileQuoter::quote_amount_out(reserve_in, reserve_out, amount_in, 30)
            .unwrap();

        assert_eq!(out, U256::from(45_330u64));
    }
}
