use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

use base_arb_common::types::DexKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexEvent {
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u64,
    pub pool_address: Address,
    pub dex: DexKind,
    pub event_type: String,
    pub raw_data_json: serde_json::Value,
}
