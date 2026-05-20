use alloy_primitives::U256;
use chrono::{Duration, Utc};
use uuid::Uuid;

use base_arb_common::types::{ArbPath, Candidate, OpportunityStatus};

pub fn build_candidate(
    block_number: u64,
    strategy: String,
    token_in: alloy_primitives::Address,
    amount_in: U256,
    expected_amount_out: U256,
    expected_profit: U256,
    min_profit: U256,
    price_impact_bps: u64,
    path: ArbPath,
    ttl_ms: i64,
) -> Candidate {
    let now = Utc::now();

    Candidate {
        id: Uuid::new_v4(),
        created_at: now,
        expires_at: now + Duration::milliseconds(ttl_ms),
        block_number,
        strategy,
        token_in,
        amount_in,
        expected_amount_out,
        expected_profit,
        min_profit,
        price_impact_bps,
        path,
        status: OpportunityStatus::Created,
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::{Duration, Utc};

    use base_arb_common::types::{ArbPath, DexKind, SwapStep};

    use super::build_candidate;

    #[test]
    fn candidate_ttl_expires_as_expected() {
        let token = address!("4200000000000000000000000000000000000006");
        let path = ArbPath {
            name: "test".into(),
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
            "ttl-test".into(),
            token,
            U256::from(100u64),
            U256::from(110u64),
            U256::from(10u64),
            U256::from(5u64),
            10,
            path,
            500,
        );

        assert!(!candidate.is_expired(candidate.created_at + Duration::milliseconds(499)));
        assert!(candidate.is_expired(candidate.created_at + Duration::milliseconds(501)));
        assert!(candidate.expires_at > Utc::now());
    }
}
