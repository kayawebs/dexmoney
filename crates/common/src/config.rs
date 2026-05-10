use alloy_primitives::Address;
use config::{Config, Environment};
use serde::Deserialize;

use crate::constants::DEFAULT_CANDIDATE_TTL_MS;

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub base_rpc_http: String,
    pub base_rpc_ws: String,
    pub postgres_url: String,
    pub redis_url: String,
    pub chain_id: u64,
    pub usdc_address: Address,
    pub weth_address: Address,
    pub aerodrome_router: Option<Address>,
    pub aerodrome_pool_factory: Option<Address>,
    pub aerodrome_slipstream_factory: Option<Address>,
    pub aerodrome_usdc_weth_pool: Option<Address>,
    pub uniswap_v3_factory: Option<Address>,
    pub uniswap_v3_router: Option<Address>,
    pub uniswap_v3_quoter: Option<Address>,
    pub uniswap_v3_usdc_weth_500_pool: Option<Address>,
    pub uniswap_v3_usdc_weth_3000_pool: Option<Address>,
    pub executor_contract: Option<Address>,
    pub eoa_address_1: Option<Address>,
    pub eoa_private_key_1: Option<String>,
    pub min_expected_profit_usdc: f64,
    pub min_simulated_profit_usdc: f64,
    pub candidate_ttl_ms: i64,
    pub max_price_impact_bps: u64,
    pub monitor_web_password: Option<String>,
}

impl Settings {
    pub fn load() -> Result<Self, config::ConfigError> {
        dotenvy::dotenv().ok();

        Config::builder()
            .set_default("candidate_ttl_ms", DEFAULT_CANDIDATE_TTL_MS)?
            .add_source(Environment::default())
            .build()?
            .try_deserialize()
    }
}
