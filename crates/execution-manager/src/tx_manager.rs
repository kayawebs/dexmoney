use alloy_primitives::{keccak256, B256, U256};

use base_arb_common::types::{Candidate, TxResult, TxStatus};

pub fn build_pending_tx(candidate: &Candidate, nonce: u64) -> TxResult {
    TxResult {
        opportunity_id: candidate.id,
        simulation_id: None,
        tx_hash: None,
        nonce,
        status: TxStatus::Pending,
        realized_profit: None,
        gas_used: None,
        effective_gas_price: Some(U256::ZERO),
        revert_reason: None,
    }
}

pub fn synthetic_tx_hash(candidate: &Candidate, nonce: u64) -> B256 {
    let payload = format!("{}:{nonce}", candidate.id);
    keccak256(payload.as_bytes())
}
