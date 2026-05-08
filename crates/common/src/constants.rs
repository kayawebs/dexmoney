use alloy_primitives::U256;

pub const BASE_CHAIN_ID: u64 = 8453;
pub const BPS_DENOMINATOR: u64 = 10_000;
pub const DEFAULT_CANDIDATE_TTL_MS: i64 = 500;

pub fn usdc_decimals() -> u8 {
    6
}

pub fn weth_decimals() -> u8 {
    18
}

pub fn zero() -> U256 {
    U256::ZERO
}
