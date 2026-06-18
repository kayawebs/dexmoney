use alloy_primitives::Address;
use config::{Config, Environment};
use serde::Deserialize;

use crate::constants::{DEFAULT_CANDIDATE_TTL_MS, DEFAULT_MAX_POOL_STATE_AGE_MS};

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub base_rpc_http: String,
    pub base_rpc_ws: String,
    pub base_rpc_flashblocks_ws: Option<String>,
    pub postgres_url: String,
    pub redis_url: String,
    pub chain_id: u64,
    pub usdc_address: Address,
    pub weth_address: Address,
    pub aerodrome_router: Option<Address>,
    pub aerodrome_pool_factory: Option<Address>,
    pub aerodrome_slipstream_router: Option<Address>,
    pub aerodrome_slipstream_factory: Option<Address>,
    pub aerodrome_usdc_weth_pool: Option<Address>,
    pub uniswap_v3_factory: Option<Address>,
    pub uniswap_v3_router: Option<Address>,
    pub uniswap_v3_quoter: Option<Address>,
    pub uniswap_v3_usdc_weth_500_pool: Option<Address>,
    pub uniswap_v3_usdc_weth_3000_pool: Option<Address>,
    pub uniswap_v4_pool_manager: Option<Address>,
    pub uniswap_v4_adapter: Option<Address>,
    pub balancer_v3_vault: Option<Address>,
    pub balancer_v3_router: Option<Address>,
    pub balancer_v3_adapter: Option<Address>,
    pub pancake_v3_factory: Option<Address>,
    pub pancake_v3_router: Option<Address>,
    pub executor_contract: Option<Address>,
    pub executor_contract_2hop: Option<Address>,
    pub executor_contract_multihop: Option<Address>,
    pub executor_owner_private_key: Option<String>,
    pub deployer_private_key: Option<String>,
    pub eoa_address_1: Option<Address>,
    pub eoa_private_key_1: Option<String>,
    pub search_amount_usdc: Option<String>,
    pub min_expected_profit_usdc: f64,
    pub min_simulated_profit_usdc: f64,
    pub candidate_ttl_ms: i64,
    pub max_pool_state_age_ms: i64,
    pub max_price_impact_bps: u64,
    pub pool_active_refresh_interval_secs: u64,
    pub pool_active_refresh_batch_size: u64,
    pub market_data_flashblocks_enabled: bool,
    pub market_data_global_pool_discovery_enabled: bool,
    pub competitor_pool_discovery_enabled: bool,
    pub competitor_collector_address: Option<Address>,
    pub competitor_pool_discovery_interval_ms: u64,
    pub competitor_pool_discovery_lookback_blocks: u64,
    pub competitor_pool_discovery_max_block_span: u64,
    pub searcher_multihop_enabled: bool,
    pub aerodrome_fee_refresh_interval_secs: u64,
    pub v3_tick_refresh_interval_secs: u64,
    pub v3_tick_bitmap_word_radius: i32,
    pub v3_quote_safety_bps: u64,
    pub quote_max_state_block_lag: u64,
    pub min_profit_failure_ttl_secs: u64,
    pub execution_min_priority_fee_wei: Option<String>,
    pub execution_priority_fee_multiplier_bps: u64,
    pub execution_max_fee_multiplier_bps: u64,
    pub execution_pending_replacement_blocks: u64,
    pub execution_replacement_fee_bump_bps: u64,
    pub execution_max_replacements: u32,
    pub execution_gas_profit_buffer_bps: u64,
    pub execution_max_candidate_lag_blocks: u64,
    pub execution_submit_enabled: bool,
    pub execution_eoa_pool_size: u64,
    pub execution_worker_min_balance_wei: Option<String>,
    pub execution_worker_target_balance_wei: Option<String>,
    pub execution_failure_rate_min_txs: u64,
    pub execution_min_success_rate_bps: u64,
    pub monitor_web_password: Option<String>,
}

impl Settings {
    pub fn load() -> Result<Self, config::ConfigError> {
        dotenvy::dotenv().ok();

        Config::builder()
            .set_default("candidate_ttl_ms", DEFAULT_CANDIDATE_TTL_MS)?
            .set_default("max_pool_state_age_ms", DEFAULT_MAX_POOL_STATE_AGE_MS)?
            .set_default("search_amount_usdc", "10,30,50,100")?
            .set_default("pool_active_refresh_interval_secs", 60u64)?
            .set_default("pool_active_refresh_batch_size", 25u64)?
            .set_default("market_data_flashblocks_enabled", true)?
            .set_default("market_data_global_pool_discovery_enabled", true)?
            .set_default("competitor_pool_discovery_enabled", true)?
            .set_default(
                "competitor_collector_address",
                "0x0629da86af5a4ae1ba5e1589b13702558d0fb056",
            )?
            .set_default("competitor_pool_discovery_interval_ms", 1000u64)?
            .set_default("competitor_pool_discovery_lookback_blocks", 100u64)?
            .set_default("competitor_pool_discovery_max_block_span", 25u64)?
            .set_default("searcher_multihop_enabled", true)?
            .set_default("aerodrome_fee_refresh_interval_secs", 3u64)?
            .set_default("v3_tick_refresh_interval_secs", 60u64)?
            .set_default("v3_tick_bitmap_word_radius", 8i32)?
            .set_default("v3_quote_safety_bps", 10u64)?
            .set_default("quote_max_state_block_lag", 0u64)?
            .set_default("min_profit_failure_ttl_secs", 21_600u64)?
            .set_default("execution_min_priority_fee_wei", "4300000")?
            .set_default("execution_priority_fee_multiplier_bps", 10_000u64)?
            .set_default("execution_max_fee_multiplier_bps", 20_000u64)?
            .set_default("execution_pending_replacement_blocks", 1u64)?
            .set_default("execution_replacement_fee_bump_bps", 15_000u64)?
            .set_default("execution_max_replacements", 4u32)?
            .set_default("execution_gas_profit_buffer_bps", 15_000u64)?
            .set_default("execution_max_candidate_lag_blocks", 1u64)?
            .set_default("execution_submit_enabled", false)?
            .set_default("execution_eoa_pool_size", 5u64)?
            .set_default("execution_worker_min_balance_wei", "200000000000000")?
            .set_default("execution_worker_target_balance_wei", "500000000000000")?
            .set_default("execution_failure_rate_min_txs", 10u64)?
            .set_default("execution_min_success_rate_bps", 2_000u64)?
            .add_source(Environment::default())
            .build()?
            .try_deserialize()
    }
}
