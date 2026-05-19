use alloy_primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Token {
    pub address: Address,
    pub symbol: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PoolId {
    pub chain_id: u64,
    pub address: Address,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DexKind {
    Aerodrome,
    UniswapV3,
    PancakeSwap,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PoolVariant {
    AerodromeVolatile,
    AerodromeSlipstream,
    UniswapV3,
    PancakeV3,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolState {
    pub pool_id: PoolId,
    pub dex: DexKind,
    pub variant: PoolVariant,
    pub token0: Address,
    pub token1: Address,
    pub fee_bps: u32,
    pub reserve0: Option<U256>,
    pub reserve1: Option<U256>,
    pub sqrt_price_x96: Option<U256>,
    pub liquidity: Option<U256>,
    pub tick: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_spacing: Option<i32>,
    pub block_number: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TickState {
    pub pool_id: PoolId,
    pub tick: i32,
    pub liquidity_net: i128,
    pub liquidity_gross: U256,
    pub block_number: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct V3LiquidityUpdate {
    pub current_tick: i32,
    pub tick_lower: i32,
    pub tick_upper: i32,
    pub amount: U256,
    pub previous_liquidity: U256,
    pub next_liquidity: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolRegistryEntry {
    pub pool_address: Address,
    pub dex: DexKind,
    pub variant: PoolVariant,
    pub token0: Address,
    pub token1: Address,
    pub fee_bps: u32,
    pub tick_spacing: Option<i32>,
    pub stable: Option<bool>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenPairSearchConfig {
    pub chain_id: u64,
    pub token0: Address,
    pub token1: Address,
    pub symbol: String,
    pub token0_search_amounts: Vec<U256>,
    pub token1_search_amounts: Vec<U256>,
    pub token0_min_profit: U256,
    pub token1_min_profit: U256,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveredPool {
    pub state: PoolState,
    pub tick_spacing: Option<i32>,
    pub stable: Option<bool>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolStateWarning {
    pub pool_address: Address,
    pub dex: DexKind,
    pub variant: PoolVariant,
    pub block_number: u64,
    pub local_state: PoolState,
    pub onchain_state: PoolState,
    pub drift_bps: u64,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolStateValidation {
    pub pool_address: Address,
    pub dex: DexKind,
    pub variant: PoolVariant,
    pub block_number: u64,
    pub block_hash: String,
    pub local_state: PoolState,
    pub onchain_state: PoolState,
    pub drift_bps: u64,
    pub passed: bool,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

impl PoolState {
    pub fn is_stale(&self, now: DateTime<Utc>, max_age_ms: i64) -> bool {
        now.signed_duration_since(self.updated_at)
            .num_milliseconds()
            > max_age_ms
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SwapStep {
    pub dex: DexKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<PoolVariant>,
    pub pool: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub fee_bps: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_spacing: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArbPath {
    pub name: String,
    pub steps: Vec<SwapStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<QuoteDiagnostics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuoteDiagnostics {
    pub modes: Vec<String>,
    pub ticks_used: u32,
    pub crossed_ticks: u32,
    pub tick_range_exhausted: bool,
    pub v3_pools_without_ticks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OpportunityStatus {
    Created,
    Rejected,
    Simulated,
    Submitted,
    Confirmed,
    Reverted,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Candidate {
    pub id: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub block_number: u64,
    pub strategy: String,
    pub token_in: Address,
    pub amount_in: U256,
    pub expected_amount_out: U256,
    pub expected_profit: U256,
    pub min_profit: U256,
    pub price_impact_bps: u64,
    pub path: ArbPath,
    pub status: OpportunityStatus,
}

impl Candidate {
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now > self.expires_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuoteResult {
    pub amount_in: U256,
    pub amount_out: U256,
    pub gas_estimate: Option<U256>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimulationResult {
    pub id: Uuid,
    pub opportunity_id: Uuid,
    pub success: bool,
    pub simulated_profit: U256,
    pub gas_estimate: Option<U256>,
    pub revert_reason: Option<String>,
    pub calldata: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TxStatus {
    Pending,
    Confirmed,
    Reverted,
    Dropped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxResult {
    pub opportunity_id: Uuid,
    pub simulation_id: Option<Uuid>,
    pub eoa: Address,
    pub tx_hash: Option<B256>,
    pub nonce: u64,
    pub status: TxStatus,
    pub realized_profit: Option<U256>,
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
    pub revert_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_json: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EoaLaneStatus {
    Idle,
    Pending,
    Blocked,
    Cooldown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EoaLaneState {
    pub address: Address,
    pub local_nonce: u64,
    pub confirmed_nonce: u64,
    pub pending_tx: Option<B256>,
    #[serde(default)]
    pub pending_opportunity_id: Option<Uuid>,
    #[serde(default)]
    pub pending_simulation_id: Option<Uuid>,
    #[serde(default)]
    pub pending_nonce: Option<u64>,
    pub eth_balance: U256,
    pub status: EoaLaneStatus,
}
