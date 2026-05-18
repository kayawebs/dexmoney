use alloy_primitives::U256;

pub const BASE_CHAIN_ID: u64 = 8453;
pub const BPS_DENOMINATOR: u64 = 10_000;
pub const DEFAULT_CANDIDATE_TTL_MS: i64 = 500;
pub const DEFAULT_MAX_POOL_STATE_AGE_MS: i64 = 7 * 24 * 60 * 60 * 1000;
pub const PANCAKE_V3_FACTORY: &str = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865";
pub const PANCAKE_V3_ROUTER: &str = "0x1b81D678ffb9C0263b24A97847620C99d213eB14";

pub fn usdc_decimals() -> u8 {
    6
}

pub fn weth_decimals() -> u8 {
    18
}

pub fn zero() -> U256 {
    U256::ZERO
}
