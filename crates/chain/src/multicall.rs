use alloy_primitives::Address;

#[derive(Debug, Clone)]
pub struct MulticallRequest {
    pub target: Address,
    pub calldata: Vec<u8>,
}
