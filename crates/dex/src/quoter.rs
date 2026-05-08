use alloy_primitives::{Address, U256};
use async_trait::async_trait;

use base_arb_common::errors::Result;
use base_arb_common::types::{PoolState, QuoteResult};

#[async_trait]
pub trait DexQuoter: Send + Sync {
    async fn quote_exact_in(
        &self,
        pool_state: &PoolState,
        token_in: Address,
        amount_in: U256,
    ) -> Result<QuoteResult>;
}
