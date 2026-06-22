use std::collections::HashSet;

use alloy_primitives::Address;
use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::Candidate;

pub fn validate_candidate(
    candidate: &Candidate,
    available_pools: &HashSet<Address>,
    _max_pool_age_ms: i64,
    min_expected_profit: alloy_primitives::U256,
    max_price_impact_bps: u64,
    whitelist_paths: &[String],
) -> Result<()> {
    let required_profit = if candidate.min_profit.is_zero() {
        min_expected_profit
    } else {
        candidate.min_profit
    };
    if candidate.expected_profit < required_profit {
        return Err(ArbBotError::RiskGate(
            "expected_profit_below_threshold".into(),
        ));
    }
    if candidate.price_impact_bps > max_price_impact_bps {
        return Err(ArbBotError::RiskGate("price_impact_too_high".into()));
    }
    if !whitelist_paths.is_empty() && !whitelist_paths.contains(&candidate.path.name) {
        return Err(ArbBotError::RiskGate("path_not_whitelisted".into()));
    }

    if candidate
        .path
        .steps
        .iter()
        .any(|step| !available_pools.contains(&step.pool))
    {
        return Err(ArbBotError::RiskGate("pool_state_missing".into()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use alloy_primitives::{address, U256};
    use chrono::Utc;

    use base_arb_common::types::{ArbPath, DexKind, PoolId, PoolState, PoolVariant, SwapStep};

    use crate::opportunity::build_candidate;

    use super::validate_candidate;

    fn sample_pool_state() -> PoolState {
        let pool = address!("4200000000000000000000000000000000000006");
        PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: DexKind::Aerodrome,
            variant: PoolVariant::AerodromeVolatile,
            factory_address: None,
            token0: pool,
            token1: address!("4200000000000000000000000000000000000001"),
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: None,
            stable: Some(false),
            reserve0: Some(U256::from(1_000u64)),
            reserve1: Some(U256::from(2_000u64)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn risk_gate_accepts_valid_candidate() {
        let token = address!("4200000000000000000000000000000000000006");
        let path = ArbPath {
            name: "usdc-weth-usdc-aero-uni".into(),
            steps: vec![SwapStep {
                dex: DexKind::Aerodrome,
                variant: None,
                factory_address: None,
                pool: token,
                token_in: token,
                token_out: token,
                fee_bps: Some(30),
                stable: Some(false),
                tick_spacing: None,
            }],
            diagnostics: None,
        };

        let candidate = build_candidate(
            1,
            "simple".into(),
            token,
            U256::from(100u64),
            U256::from(101u64),
            U256::from(1u64),
            U256::from(1u64),
            20,
            path,
            500,
        );

        let result = validate_candidate(
            &candidate,
            &HashSet::from([sample_pool_state().pool_id.address]),
            5_000,
            U256::from(1u64),
            50,
            &["usdc-weth-usdc-aero-uni".into()],
        );

        assert!(result.is_ok());
    }
}
