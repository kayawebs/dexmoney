use alloy_primitives::{Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{PoolState, QuoteResult};

use crate::quoter::DexQuoter;

#[derive(Debug, Clone, Default)]
pub struct UniswapV3ChainQuoter;

#[async_trait]
impl DexQuoter for UniswapV3ChainQuoter {
    async fn quote_exact_in(
        &self,
        pool_state: &PoolState,
        token_in: Address,
        amount_in: U256,
    ) -> Result<QuoteResult> {
        if token_in != pool_state.token0 && token_in != pool_state.token1 {
            return Err(ArbBotError::Quote("token_in not in pool".into()));
        }

        // Placeholder for quoter/eth_call integration.
        // Uses a deterministic spread so the end-to-end search pipeline can be exercised offline.
        let amount_out = amount_in
            .checked_mul(U256::from(10_015u64))
            .and_then(|v| v.checked_div(U256::from(10_000u64)))
            .ok_or_else(|| ArbBotError::Quote("quote overflow".into()))?;

        Ok(QuoteResult {
            amount_in,
            amount_out,
            gas_estimate: None,
        })
    }
}
