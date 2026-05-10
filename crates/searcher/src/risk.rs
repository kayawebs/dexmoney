use chrono::Utc;

use base_arb_common::errors::{ArbBotError, Result};
use base_arb_common::types::{Candidate, PoolState};

pub fn validate_candidate(
    candidate: &Candidate,
    pool_states: &[PoolState],
    max_pool_age_ms: i64,
    min_expected_profit: alloy_primitives::U256,
    max_price_impact_bps: u64,
    whitelist_paths: &[String],
) -> Result<()> {
    if candidate.expected_profit < min_expected_profit {
        return Err(ArbBotError::RiskGate(
            "expected profit below threshold".into(),
        ));
    }
    if candidate.price_impact_bps > max_price_impact_bps {
        return Err(ArbBotError::RiskGate("price impact too high".into()));
    }
    if !whitelist_paths.is_empty() && !whitelist_paths.contains(&candidate.path.name) {
        return Err(ArbBotError::RiskGate("path not whitelisted".into()));
    }

    let now = Utc::now();
    if candidate.path.steps.iter().any(|step| {
        match pool_states
            .iter()
            .find(|state| state.pool_id.address == step.pool)
        {
            Some(state) => state.is_stale(now, max_pool_age_ms),
            None => true,
        }
    }) {
        return Err(ArbBotError::RiskGate("pool state stale".into()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
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
            token0: pool,
            token1: address!("4200000000000000000000000000000000000001"),
            fee_bps: 30,
            reserve0: Some(U256::from(1_000u64)),
            reserve1: Some(U256::from(2_000u64)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            block_number: 1,
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
                pool: token,
                token_in: token,
                token_out: token,
                fee_bps: Some(30),
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
            &[sample_pool_state()],
            5_000,
            U256::from(1u64),
            50,
            &["usdc-weth-usdc-aero-uni".into()],
        );

        assert!(result.is_ok());
    }
}
