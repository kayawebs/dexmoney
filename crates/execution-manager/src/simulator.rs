use alloy_primitives::U256;

use base_arb_common::types::{Candidate, SimulationResult};

pub fn simulate(candidate: &Candidate, min_simulated_profit_usdc: f64) -> SimulationResult {
    let min_profit_units = (min_simulated_profit_usdc * 1_000_000.0) as u64;
    let expired = candidate.is_expired(chrono::Utc::now());
    let success = !expired && candidate.expected_profit >= U256::from(min_profit_units);

    SimulationResult {
        opportunity_id: candidate.id,
        success,
        simulated_profit: if success {
            candidate.expected_profit
        } else {
            U256::ZERO
        },
        gas_estimate: Some(U256::from(150_000u64)),
        revert_reason: if expired {
            Some("candidate expired".into())
        } else if !success {
            Some("profit below simulated threshold".into())
        } else {
            None
        },
        calldata: b"executeWithOwnFunds".to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use base_arb_common::types::{ArbPath, OpportunityStatus};

    use super::simulate;

    #[test]
    fn simulator_rejects_expired_candidate() {
        let candidate = base_arb_common::types::Candidate {
            id: Uuid::new_v4(),
            created_at: Utc::now() - Duration::seconds(2),
            expires_at: Utc::now() - Duration::seconds(1),
            block_number: 1,
            strategy: "demo".into(),
            token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            amount_in: U256::from(1u64),
            expected_amount_out: U256::from(2u64),
            expected_profit: U256::from(1u64),
            min_profit: U256::from(1u64),
            price_impact_bps: 1,
            path: ArbPath {
                name: "demo".into(),
                steps: Vec::new(),
            },
            status: OpportunityStatus::Created,
        };

        let result = simulate(&candidate, 0.005);
        assert!(!result.success);
        assert_eq!(result.revert_reason.as_deref(), Some("candidate expired"));
    }
}
