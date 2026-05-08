use alloy_primitives::Address;

#[derive(Debug, Clone)]
pub struct ContractAddresses {
    pub aerodrome_router: Option<Address>,
    pub uniswap_v3_router: Option<Address>,
    pub uniswap_v3_quoter: Option<Address>,
    pub executor_contract: Option<Address>,
}
