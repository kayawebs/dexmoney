use crate::events::DexEvent;
use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use base_arb_common::config::Settings;
use base_arb_common::constants::PANCAKE_V3_FACTORY;
use base_arb_common::types::{
    DexKind, DiscoveredPool, PoolId, PoolRegistryEntry, PoolState, PoolVariant, TickState,
};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use tracing::{debug, info};

const AERODROME_POOL_FACTORY: &str = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da";
const AERODROME_SLIPSTREAM_FACTORIES: [&str; 2] = [
    "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
    "0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a",
];
const UNISWAP_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";
const UNISWAP_V3_FEE_TIERS: [u32; 4] = [100, 500, 3000, 10000];
const PANCAKE_V3_FEE_TIERS: [u32; 4] = [100, 500, 2500, 10000];
const FALLBACK_SLIPSTREAM_TICK_SPACINGS: [i32; 7] = [1, 10, 50, 100, 200, 500, 2000];

#[derive(Debug, Clone)]
pub struct ChainProvider {
    pub http_url: String,
    pub ws_url: String,
    pub chain_id: u64,
    client: reqwest::Client,
}

impl ChainProvider {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            http_url: settings.base_rpc_http.clone(),
            ws_url: settings.base_rpc_ws.clone(),
            chain_id: settings.chain_id,
            client: reqwest::Client::new(),
        }
    }

    pub async fn healthcheck(&self) -> Result<()> {
        info!(http = %self.http_url, ws = %self.ws_url, chain_id = self.chain_id, "chain provider configured");
        Ok(())
    }

    pub async fn bootstrap_configured_pools(&self, settings: &Settings) -> Result<Vec<PoolState>> {
        let now = Utc::now();
        let mut out = Vec::new();

        if let Some(pool) = settings.aerodrome_usdc_weth_pool {
            let state = self
                .fetch_aerodrome_state(pool)
                .await
                .with_context(|| {
                    format!(
                        "failed to initialize AERODROME_USDC_WETH_POOL {pool:#x}; expected either volatile getReserves() or Slipstream slot0()/liquidity()"
                    )
                })?;
            out.push(state);
        }

        for (pool, fee_bps) in [
            (settings.uniswap_v3_usdc_weth_500_pool, 5u32),
            (settings.uniswap_v3_usdc_weth_3000_pool, 30u32),
        ] {
            if let Some(pool) = pool {
                let (token0, token1) = self.fetch_pool_tokens(pool).await.with_context(|| {
                    format!("failed to read token0/token1 for Uniswap V3 pool {pool:#x}")
                })?;
                let (sqrt_price_x96, tick, liquidity) =
                    self.fetch_uniswap_v3_state(pool).await.with_context(|| {
                        format!(
                            "failed to initialize Uniswap V3 USDC/WETH pool {pool:#x} fee_bps={fee_bps}; expected a V3 pool supporting slot0() and liquidity()"
                        )
                    })?;
                out.push(PoolState {
                    pool_id: PoolId {
                        chain_id: self.chain_id,
                        address: pool,
                    },
                    dex: DexKind::UniswapV3,
                    variant: PoolVariant::UniswapV3,
                    factory_address: settings.uniswap_v3_factory,
                    token0,
                    token1,
                    token0_decimals: self.fetch_token_decimals(token0).await.ok(),
                    token1_decimals: self.fetch_token_decimals(token1).await.ok(),
                    fee_bps,
                    fee_pips: Some(fee_bps.saturating_mul(100)),
                    stable: None,
                    reserve0: None,
                    reserve1: None,
                    sqrt_price_x96: Some(sqrt_price_x96),
                    liquidity: Some(liquidity),
                    tick: Some(tick),
                    tick_spacing: None,
                    block_number: self.get_block_number().await?,
                    updated_at: now,
                });
            }
        }

        if out.is_empty() {
            anyhow::bail!(
                "no configured pools found; set AERODROME_USDC_WETH_POOL and at least one Uniswap V3 USDC/WETH pool in .env"
            );
        }

        Ok(out)
    }

    pub async fn fetch_pool_state_from_registry(
        &self,
        entry: &PoolRegistryEntry,
    ) -> Result<PoolState> {
        self.fetch_pool_state_from_registry_at_block(entry, None)
            .await
    }

    pub async fn fetch_pool_state_from_registry_at_block(
        &self,
        entry: &PoolRegistryEntry,
        block_number: Option<u64>,
    ) -> Result<PoolState> {
        match entry.dex {
            DexKind::Aerodrome => {
                let mut state = self
                    .fetch_aerodrome_pool_state_at_block(entry.pool_address, block_number)
                    .await?;
                state.tick_spacing = entry.tick_spacing;
                state.factory_address = entry.factory_address;
                state.stable = entry.stable;
                if entry.variant == PoolVariant::AerodromeSlipstream {
                    state.fee_pips = self
                        .fetch_aerodrome_slipstream_fee_pips_for_entry(entry)
                        .await
                        .ok();
                    if let Some(fee_pips) = state.fee_pips {
                        state.fee_bps = fee_pips / 100;
                    }
                } else {
                    state.fee_bps = self
                        .fetch_aerodrome_classic_fee_bps_for_entry(entry)
                        .await
                        .unwrap_or(entry.fee_bps);
                }
                Ok(state)
            }
            DexKind::UniswapV3 | DexKind::PancakeSwap => {
                let (token0, token1) = self
                    .fetch_pool_tokens(entry.pool_address)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read token0/token1 for V3 registry pool {:#x}",
                            entry.pool_address
                        )
                    })?;
                let (sqrt_price_x96, tick, liquidity) = self
                    .fetch_uniswap_v3_state_at_block(entry.pool_address, block_number)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read state for V3 registry pool {:#x}",
                            entry.pool_address
                        )
                    })?;
                Ok(PoolState {
                    pool_id: PoolId {
                        chain_id: self.chain_id,
                        address: entry.pool_address,
                    },
                    dex: entry.dex,
                    variant: entry.variant,
                    factory_address: entry.factory_address,
                    token0,
                    token1,
                    token0_decimals: self.fetch_token_decimals(token0).await.ok(),
                    token1_decimals: self.fetch_token_decimals(token1).await.ok(),
                    fee_bps: entry.fee_bps,
                    fee_pips: Some(entry.fee_bps.saturating_mul(100)),
                    stable: entry.stable,
                    reserve0: None,
                    reserve1: None,
                    sqrt_price_x96: Some(sqrt_price_x96),
                    liquidity: Some(liquidity),
                    tick: Some(tick),
                    tick_spacing: entry.tick_spacing,
                    block_number: block_number.unwrap_or(self.get_block_number().await?),
                    updated_at: Utc::now(),
                })
            }
        }
    }

    pub async fn fetch_pool_state_from_registry_at_block_hash(
        &self,
        entry: &PoolRegistryEntry,
        block_hash: &str,
        block_number: u64,
    ) -> Result<PoolState> {
        match entry.dex {
            DexKind::Aerodrome => {
                let mut state = self
                    .fetch_aerodrome_pool_state_at_block_hash(
                        entry.pool_address,
                        block_hash,
                        block_number,
                    )
                    .await?;
                state.tick_spacing = entry.tick_spacing;
                state.factory_address = entry.factory_address;
                state.stable = entry.stable;
                if entry.variant == PoolVariant::AerodromeSlipstream {
                    state.fee_pips = self
                        .fetch_aerodrome_slipstream_fee_pips_for_entry(entry)
                        .await
                        .ok();
                    if let Some(fee_pips) = state.fee_pips {
                        state.fee_bps = fee_pips / 100;
                    }
                } else {
                    state.fee_bps = self
                        .fetch_aerodrome_classic_fee_bps_for_entry(entry)
                        .await
                        .unwrap_or(entry.fee_bps);
                }
                Ok(state)
            }
            DexKind::UniswapV3 | DexKind::PancakeSwap => {
                let (token0, token1) = self
                    .fetch_pool_tokens(entry.pool_address)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read token0/token1 for V3 registry pool {:#x}",
                            entry.pool_address
                        )
                    })?;
                let (sqrt_price_x96, tick, liquidity) = self
                    .fetch_uniswap_v3_state_at_block_hash(entry.pool_address, block_hash)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to read state for V3 registry pool {:#x} at block_hash {block_hash}",
                            entry.pool_address
                        )
                    })?;
                Ok(PoolState {
                    pool_id: PoolId {
                        chain_id: self.chain_id,
                        address: entry.pool_address,
                    },
                    dex: entry.dex,
                    variant: entry.variant,
                    factory_address: entry.factory_address,
                    token0,
                    token1,
                    token0_decimals: self.fetch_token_decimals(token0).await.ok(),
                    token1_decimals: self.fetch_token_decimals(token1).await.ok(),
                    fee_bps: entry.fee_bps,
                    fee_pips: Some(entry.fee_bps.saturating_mul(100)),
                    stable: entry.stable,
                    reserve0: None,
                    reserve1: None,
                    sqrt_price_x96: Some(sqrt_price_x96),
                    liquidity: Some(liquidity),
                    tick: Some(tick),
                    tick_spacing: entry.tick_spacing,
                    block_number,
                    updated_at: Utc::now(),
                })
            }
        }
    }

    pub async fn discover_pools_for_pair(
        &self,
        settings: &Settings,
        token_a: Address,
        token_b: Address,
    ) -> Result<Vec<DiscoveredPool>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();

        self.discover_aerodrome_classic(settings, token_a, token_b, &mut seen, &mut out)
            .await?;
        self.discover_aerodrome_slipstream(settings, token_a, token_b, &mut seen, &mut out)
            .await?;
        self.discover_uniswap_v3(settings, token_a, token_b, &mut seen, &mut out)
            .await?;
        self.discover_pancake_v3(settings, token_a, token_b, &mut seen, &mut out)
            .await?;

        Ok(out)
    }

    pub async fn fetch_erc20_symbol(&self, token: Address) -> Result<String> {
        let raw = self.eth_call(token, "0x95d89b41", "ERC20 symbol()").await?;
        decode_abi_string_or_bytes32(&raw)
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        let value = self.rpc("eth_blockNumber", json!([])).await?;
        parse_hex_u64(value.as_str().unwrap_or("0x0"))
    }

    pub async fn get_block_hash(&self, block_number: u64) -> Result<String> {
        let value = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{block_number:x}"), false]),
            )
            .await?;
        let hash = value
            .get("hash")
            .and_then(Value::as_str)
            .context("eth_getBlockByNumber returned no hash")?;
        Ok(hash.to_string())
    }

    pub async fn get_transaction_count(&self, address: Address, pending: bool) -> Result<u64> {
        let tag = if pending { "pending" } else { "latest" };
        let value = self
            .rpc(
                "eth_getTransactionCount",
                json!([format!("{address:#x}"), tag]),
            )
            .await?;
        parse_hex_u64(value.as_str().unwrap_or("0x0"))
    }

    pub async fn get_balance(&self, address: Address) -> Result<U256> {
        let value = self
            .rpc("eth_getBalance", json!([format!("{address:#x}"), "latest"]))
            .await?;
        parse_hex_u256(value.as_str().unwrap_or("0x0"))
    }

    pub async fn estimate_gas(&self, from: Address, to: Address, data: &str) -> Result<U256> {
        let call = json!({
            "from": format!("{from:#x}"),
            "to": format!("{to:#x}"),
            "data": data,
        });
        let value = self.rpc("eth_estimateGas", json!([call])).await?;
        parse_hex_u256(value.as_str().unwrap_or("0x0"))
    }

    pub async fn suggested_eip1559_fees(&self) -> Result<(U256, U256)> {
        let priority = match self.rpc("eth_maxPriorityFeePerGas", json!([])).await {
            Ok(value) => parse_hex_u256(value.as_str().unwrap_or("0x0"))?,
            Err(_) => U256::from(1_000_000u64),
        };
        let latest = self
            .rpc("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let base_fee = latest
            .get("baseFeePerGas")
            .and_then(Value::as_str)
            .map(parse_hex_u256)
            .transpose()?
            .unwrap_or(U256::ZERO);
        let max_fee = base_fee
            .saturating_mul(U256::from(2u64))
            .saturating_add(priority);
        Ok((max_fee, priority))
    }

    pub async fn send_raw_transaction(&self, raw_tx: &str) -> Result<B256> {
        let value = self.rpc("eth_sendRawTransaction", json!([raw_tx])).await?;
        value
            .as_str()
            .context("eth_sendRawTransaction returned non-string tx hash")?
            .parse()
            .context("failed to parse tx hash")
    }

    pub async fn get_transaction_receipt(&self, tx_hash: B256) -> Result<Option<TxReceipt>> {
        let value = self
            .rpc(
                "eth_getTransactionReceipt",
                json!([format!("{tx_hash:#x}")]),
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }

        let status = value
            .get("status")
            .and_then(Value::as_str)
            .map(parse_hex_u64)
            .transpose()?
            .unwrap_or_default();
        let gas_used = value
            .get("gasUsed")
            .and_then(Value::as_str)
            .map(parse_hex_u256)
            .transpose()?;
        let effective_gas_price = value
            .get("effectiveGasPrice")
            .and_then(Value::as_str)
            .map(parse_hex_u256)
            .transpose()?;
        let block_number = value
            .get("blockNumber")
            .and_then(Value::as_str)
            .map(parse_hex_u64)
            .transpose()?;

        Ok(Some(TxReceipt {
            tx_hash,
            success: status == 1,
            gas_used,
            effective_gas_price,
            block_number,
            raw: value,
        }))
    }

    async fn discover_aerodrome_classic(
        &self,
        settings: &Settings,
        token_a: Address,
        token_b: Address,
        seen: &mut HashSet<Address>,
        out: &mut Vec<DiscoveredPool>,
    ) -> Result<()> {
        let factory = settings
            .aerodrome_pool_factory
            .unwrap_or(AERODROME_POOL_FACTORY.parse()?);

        for stable in [false, true] {
            let data = encode_get_pool_bool(token_a, token_b, stable);
            match self
                .eth_call(
                    factory,
                    &data,
                    "Aerodrome factory getPool(address,address,bool)",
                )
                .await
            {
                Ok(raw) => {
                    let pool = decode_single_address(&raw)?;
                    if pool == Address::ZERO || !seen.insert(pool) {
                        continue;
                    }
                    let mut state = match self.fetch_aerodrome_pool_state(pool).await.with_context(
                        || format!("Aerodrome classic discovered pool {pool:#x} is not readable"),
                    ) {
                        Ok(state) => state,
                        Err(err) => {
                            debug!(pool = %pool, error = %err, "Aerodrome classic discovered pool skipped");
                            continue;
                        }
                    };
                    state.factory_address = Some(factory);
                    state.stable = Some(stable);
                    state.fee_bps = self
                        .fetch_aerodrome_classic_fee_bps(factory, pool, stable)
                        .await
                        .unwrap_or(state.fee_bps);
                    out.push(DiscoveredPool {
                        state,
                        factory_address: Some(factory),
                        tick_spacing: None,
                        stable: Some(stable),
                        source: "aerodrome_classic_factory".to_string(),
                    });
                }
                Err(err) => {
                    debug!(factory = %factory, stable, error = %err, "Aerodrome classic discovery probe failed")
                }
            }
        }

        Ok(())
    }

    async fn discover_aerodrome_slipstream(
        &self,
        settings: &Settings,
        token_a: Address,
        token_b: Address,
        seen: &mut HashSet<Address>,
        out: &mut Vec<DiscoveredPool>,
    ) -> Result<()> {
        let factories = slipstream_factories(settings)?;

        for factory in factories {
            let tick_spacings = self
                .fetch_slipstream_tick_spacings(factory)
                .await
                .unwrap_or_else(|err| {
                    info!(factory = %factory, error = %err, "Aerodrome Slipstream tickSpacings() failed; using fallback tick spacings");
                    FALLBACK_SLIPSTREAM_TICK_SPACINGS.to_vec()
                });

            for tick_spacing in tick_spacings {
                let data = encode_get_pool_int24(token_a, token_b, tick_spacing);
                match self
                    .eth_call(
                        factory,
                        &data,
                        "Aerodrome Slipstream getPool(address,address,int24)",
                    )
                    .await
                {
                    Ok(raw) => {
                        let pool = decode_single_address(&raw)?;
                        if pool == Address::ZERO || !seen.insert(pool) {
                            continue;
                        }
                        let mut state = match self
                            .fetch_aerodrome_pool_state(pool)
                            .await
                            .with_context(|| {
                                format!(
                                    "Aerodrome Slipstream discovered pool {pool:#x} is not readable"
                                )
                            }) {
                            Ok(state) => state,
                            Err(err) => {
                                debug!(pool = %pool, error = %err, "Aerodrome Slipstream discovered pool skipped");
                                continue;
                            }
                        };
                        state.tick_spacing = Some(tick_spacing);
                        state.factory_address = Some(factory);
                        state.stable = None;
                        state.fee_pips = self
                            .fetch_aerodrome_slipstream_fee_pips(factory, pool)
                            .await
                            .ok();
                        if let Some(fee_pips) = state.fee_pips {
                            state.fee_bps = fee_pips / 100;
                        }
                        out.push(DiscoveredPool {
                            state,
                            factory_address: Some(factory),
                            tick_spacing: Some(tick_spacing),
                            stable: None,
                            source: "aerodrome_slipstream_factory".to_string(),
                        });
                    }
                    Err(err) => {
                        debug!(factory = %factory, tick_spacing, error = %err, "Aerodrome Slipstream discovery probe failed")
                    }
                }
            }
        }

        Ok(())
    }

    async fn discover_uniswap_v3(
        &self,
        settings: &Settings,
        token_a: Address,
        token_b: Address,
        seen: &mut HashSet<Address>,
        out: &mut Vec<DiscoveredPool>,
    ) -> Result<()> {
        let factory = settings
            .uniswap_v3_factory
            .unwrap_or(UNISWAP_V3_FACTORY.parse()?);

        for fee in UNISWAP_V3_FEE_TIERS {
            let data = encode_get_pool_uint24(token_a, token_b, fee);
            match self
                .eth_call(
                    factory,
                    &data,
                    "Uniswap V3 factory getPool(address,address,uint24)",
                )
                .await
            {
                Ok(raw) => {
                    let pool = decode_single_address(&raw)?;
                    if pool == Address::ZERO || !seen.insert(pool) {
                        continue;
                    }
                    let (token0, token1) = match self.fetch_pool_tokens(pool).await {
                        Ok(tokens) => tokens,
                        Err(err) => {
                            debug!(pool = %pool, error = %err, "Uniswap V3 discovered pool token read failed; skipping");
                            continue;
                        }
                    };
                    let (sqrt_price_x96, tick, liquidity) = match self
                        .fetch_uniswap_v3_state(pool)
                        .await
                    {
                        Ok(state) => state,
                        Err(err) => {
                            debug!(pool = %pool, error = %err, "Uniswap V3 discovered pool state read failed; skipping");
                            continue;
                        }
                    };
                    out.push(DiscoveredPool {
                        state: PoolState {
                            pool_id: PoolId {
                                chain_id: self.chain_id,
                                address: pool,
                            },
                            dex: DexKind::UniswapV3,
                            variant: PoolVariant::UniswapV3,
                            factory_address: Some(factory),
                            token0,
                            token1,
                            token0_decimals: self.fetch_token_decimals(token0).await.ok(),
                            token1_decimals: self.fetch_token_decimals(token1).await.ok(),
                            fee_bps: fee / 100,
                            fee_pips: Some(fee),
                            stable: None,
                            reserve0: None,
                            reserve1: None,
                            sqrt_price_x96: Some(sqrt_price_x96),
                            liquidity: Some(liquidity),
                            tick: Some(tick),
                            tick_spacing: None,
                            block_number: self.get_block_number().await?,
                            updated_at: Utc::now(),
                        },
                        factory_address: Some(factory),
                        tick_spacing: None,
                        stable: None,
                        source: format!("uniswap_v3_factory_fee_{fee}"),
                    });
                }
                Err(err) => {
                    debug!(factory = %factory, fee, error = %err, "Uniswap V3 discovery probe failed")
                }
            }
        }

        Ok(())
    }

    async fn discover_pancake_v3(
        &self,
        settings: &Settings,
        token_a: Address,
        token_b: Address,
        seen: &mut HashSet<Address>,
        out: &mut Vec<DiscoveredPool>,
    ) -> Result<()> {
        let factory = settings
            .pancake_v3_factory
            .unwrap_or(PANCAKE_V3_FACTORY.parse()?);

        for fee in PANCAKE_V3_FEE_TIERS {
            let data = encode_get_pool_uint24(token_a, token_b, fee);
            match self
                .eth_call(
                    factory,
                    &data,
                    "Pancake V3 factory getPool(address,address,uint24)",
                )
                .await
            {
                Ok(raw) => {
                    let pool = decode_single_address(&raw)?;
                    if pool == Address::ZERO || !seen.insert(pool) {
                        continue;
                    }
                    let (token0, token1) = match self.fetch_pool_tokens(pool).await {
                        Ok(tokens) => tokens,
                        Err(err) => {
                            debug!(pool = %pool, error = %err, "Pancake V3 discovered pool token read failed; skipping");
                            continue;
                        }
                    };
                    let (sqrt_price_x96, tick, liquidity) = match self
                        .fetch_uniswap_v3_state(pool)
                        .await
                    {
                        Ok(state) => state,
                        Err(err) => {
                            debug!(pool = %pool, error = %err, "Pancake V3 discovered pool state read failed; skipping");
                            continue;
                        }
                    };
                    out.push(DiscoveredPool {
                        state: PoolState {
                            pool_id: PoolId {
                                chain_id: self.chain_id,
                                address: pool,
                            },
                            dex: DexKind::PancakeSwap,
                            variant: PoolVariant::PancakeV3,
                            factory_address: Some(factory),
                            token0,
                            token1,
                            token0_decimals: self.fetch_token_decimals(token0).await.ok(),
                            token1_decimals: self.fetch_token_decimals(token1).await.ok(),
                            fee_bps: fee / 100,
                            fee_pips: Some(fee),
                            stable: None,
                            reserve0: None,
                            reserve1: None,
                            sqrt_price_x96: Some(sqrt_price_x96),
                            liquidity: Some(liquidity),
                            tick: Some(tick),
                            tick_spacing: None,
                            block_number: self.get_block_number().await?,
                            updated_at: Utc::now(),
                        },
                        factory_address: Some(factory),
                        tick_spacing: None,
                        stable: None,
                        source: format!("pancake_v3_factory_fee_{fee}"),
                    });
                }
                Err(err) => {
                    debug!(factory = %factory, fee, error = %err, "Pancake V3 discovery probe failed")
                }
            }
        }

        Ok(())
    }

    async fn fetch_slipstream_tick_spacings(&self, factory: Address) -> Result<Vec<i32>> {
        let raw = self
            .eth_call(factory, "0x9cbbbe86", "Aerodrome Slipstream tickSpacings()")
            .await?;
        decode_int24_array(&raw)
    }

    pub async fn fetch_relevant_events_for_pools(
        &self,
        pools: &[PoolState],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DexEvent>> {
        if from_block > to_block {
            return Ok(Vec::new());
        }

        let mut seen_addresses = HashSet::new();
        let addresses = pools
            .iter()
            .filter_map(|pool| {
                let address = format!("{:#x}", pool.pool_id.address);
                seen_addresses.insert(address.clone()).then_some(address)
            })
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        let params = json!([{
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
            "address": addresses,
        }]);
        let value = self.rpc("eth_getLogs", params).await?;
        let logs: Vec<RpcLog> = serde_json::from_value(value)?;

        let mut out = Vec::with_capacity(logs.len());
        let mut seen_logs = HashSet::new();
        for log in logs {
            let log_index = parse_hex_u64(&log.log_index)?;
            if !seen_logs.insert((log.transaction_hash.clone(), log_index)) {
                continue;
            }
            let raw_data_json = serde_json::to_value(&log)?;
            let pool_address: Address = log.address.parse()?;
            let dex = dex_for_pool(pools, pool_address);
            let event_type = decode_event_type(dex, log.topics.first().map(String::as_str));
            out.push(DexEvent {
                block_number: parse_hex_u64(&log.block_number)?,
                tx_hash: log.transaction_hash,
                log_index,
                pool_address,
                dex,
                event_type,
                raw_data_json,
            });
        }
        out.sort_by_key(|event| (event.block_number, event.log_index));
        Ok(out)
    }

    async fn fetch_aerodrome_state(&self, pool: Address) -> Result<PoolState> {
        match self.fetch_aerodrome_pool_state(pool).await {
            Ok(state) => Ok(state),
            Err(pool_err) => {
                let resolved_pool = self.resolve_aerodrome_gauge_pool(pool).await.with_context(|| {
                    format!("configured address {pool:#x} is not readable as an Aerodrome pool, and gauge pool resolution failed; direct pool read error: {pool_err}")
                })?;
                info!(
                    configured_address = %pool,
                    resolved_pool = %resolved_pool,
                    "resolved Aerodrome gauge to pool"
                );
                self.fetch_aerodrome_pool_state(resolved_pool)
                    .await
                    .with_context(|| {
                        format!(
                            "resolved Aerodrome gauge {pool:#x} to pool {resolved_pool:#x}, but resolved pool state read failed"
                        )
                    })
            }
        }
    }

    async fn fetch_aerodrome_pool_state(&self, pool: Address) -> Result<PoolState> {
        self.fetch_aerodrome_pool_state_at_block(pool, None).await
    }

    async fn fetch_aerodrome_pool_state_at_block(
        &self,
        pool: Address,
        block_number: Option<u64>,
    ) -> Result<PoolState> {
        let block_number = block_number.unwrap_or(self.get_block_number().await?);
        let (token0, token1) = self.fetch_pool_tokens(pool).await.with_context(|| {
            format!("failed to read token0/token1 for Aerodrome pool {pool:#x}")
        })?;
        let token0_decimals = self.fetch_token_decimals(token0).await.ok();
        let token1_decimals = self.fetch_token_decimals(token1).await.ok();

        match self
            .fetch_aerodrome_reserves_at_block(pool, Some(block_number))
            .await
        {
            Ok((reserve0, reserve1)) => Ok(PoolState {
                pool_id: PoolId {
                    chain_id: self.chain_id,
                    address: pool,
                },
                dex: DexKind::Aerodrome,
                variant: PoolVariant::AerodromeVolatile,
                factory_address: None,
                token0,
                token1,
                token0_decimals,
                token1_decimals,
                fee_bps: 30,
                fee_pips: None,
                stable: None,
                reserve0: Some(reserve0),
                reserve1: Some(reserve1),
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                tick_spacing: None,
                block_number,
                updated_at: Utc::now(),
            }),
            Err(reserve_err) => {
                let (sqrt_price_x96, tick, liquidity) = self
                    .fetch_aerodrome_slipstream_state_at_block(pool, Some(block_number))
                    .await
                    .with_context(|| {
                        format!(
                            "volatile getReserves() failed first: {reserve_err}; Slipstream fallback also failed"
                        )
                    })?;

                Ok(PoolState {
                    pool_id: PoolId {
                        chain_id: self.chain_id,
                        address: pool,
                    },
                    dex: DexKind::Aerodrome,
                    variant: PoolVariant::AerodromeSlipstream,
                    factory_address: None,
                    token0,
                    token1,
                    token0_decimals,
                    token1_decimals,
                    fee_bps: 30,
                    fee_pips: None,
                    stable: None,
                    reserve0: None,
                    reserve1: None,
                    sqrt_price_x96: Some(sqrt_price_x96),
                    liquidity: Some(liquidity),
                    tick: Some(tick),
                    tick_spacing: None,
                    block_number,
                    updated_at: Utc::now(),
                })
            }
        }
    }

    async fn fetch_aerodrome_pool_state_at_block_hash(
        &self,
        pool: Address,
        block_hash: &str,
        block_number: u64,
    ) -> Result<PoolState> {
        let (token0, token1) = self.fetch_pool_tokens(pool).await.with_context(|| {
            format!("failed to read token0/token1 for Aerodrome pool {pool:#x}")
        })?;
        let token0_decimals = self.fetch_token_decimals(token0).await.ok();
        let token1_decimals = self.fetch_token_decimals(token1).await.ok();

        match self
            .fetch_aerodrome_reserves_at_block_hash(pool, block_hash)
            .await
        {
            Ok((reserve0, reserve1)) => Ok(PoolState {
                pool_id: PoolId {
                    chain_id: self.chain_id,
                    address: pool,
                },
                dex: DexKind::Aerodrome,
                variant: PoolVariant::AerodromeVolatile,
                factory_address: None,
                token0,
                token1,
                token0_decimals,
                token1_decimals,
                fee_bps: 30,
                fee_pips: None,
                stable: None,
                reserve0: Some(reserve0),
                reserve1: Some(reserve1),
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                tick_spacing: None,
                block_number,
                updated_at: Utc::now(),
            }),
            Err(reserve_err) => {
                let (sqrt_price_x96, tick, liquidity) = self
                    .fetch_aerodrome_slipstream_state_at_block_hash(pool, block_hash)
                    .await
                    .with_context(|| {
                        format!(
                            "volatile getReserves() failed first: {reserve_err}; Slipstream fallback also failed"
                        )
                    })?;

                Ok(PoolState {
                    pool_id: PoolId {
                        chain_id: self.chain_id,
                        address: pool,
                    },
                    dex: DexKind::Aerodrome,
                    variant: PoolVariant::AerodromeSlipstream,
                    factory_address: None,
                    token0,
                    token1,
                    token0_decimals,
                    token1_decimals,
                    fee_bps: 30,
                    fee_pips: None,
                    stable: None,
                    reserve0: None,
                    reserve1: None,
                    sqrt_price_x96: Some(sqrt_price_x96),
                    liquidity: Some(liquidity),
                    tick: Some(tick),
                    tick_spacing: None,
                    block_number,
                    updated_at: Utc::now(),
                })
            }
        }
    }

    async fn resolve_aerodrome_gauge_pool(&self, gauge: Address) -> Result<Address> {
        for (selector, label) in [
            ("0x16f0115b", "pool()"),
            ("0x72f702f3", "stakingToken()"),
            ("0xcc7a262e", "stakedToken()"),
        ] {
            match self.eth_call(gauge, selector, label).await {
                Ok(raw) => {
                    let words = decode_32byte_words(&raw)?;
                    let candidate = parse_word_address(&words[0])?;
                    if candidate != Address::ZERO {
                        return Ok(candidate);
                    }
                }
                Err(err) => {
                    info!(
                        gauge = %gauge,
                        selector,
                        label,
                        error = %err,
                        "Aerodrome gauge pool resolver probe failed"
                    );
                }
            }
        }

        anyhow::bail!("could not resolve Aerodrome gauge {gauge:#x} to an underlying pool")
    }

    async fn fetch_pool_tokens(&self, pool: Address) -> Result<(Address, Address)> {
        let token0_raw = self.eth_call(pool, "0x0dfe1681", "token0()").await?;
        let token1_raw = self.eth_call(pool, "0xd21220a7", "token1()").await?;
        let token0_words = decode_32byte_words(&token0_raw)?;
        let token1_words = decode_32byte_words(&token1_raw)?;
        Ok((
            parse_word_address(&token0_words[0])?,
            parse_word_address(&token1_words[0])?,
        ))
    }

    async fn fetch_token_decimals(&self, token: Address) -> Result<u8> {
        let raw = self.eth_call(token, "0x313ce567", "decimals()").await?;
        let words = decode_32byte_words(&raw)?;
        let decimals = parse_word_u256(&words[0])?;
        let decimals_u64 = u64::try_from(decimals)
            .map_err(|_| anyhow::anyhow!("token decimals too large for {token:#x}: {decimals}"))?;
        u8::try_from(decimals_u64)
            .map_err(|_| anyhow::anyhow!("token decimals too large for {token:#x}: {decimals_u64}"))
    }

    async fn fetch_aerodrome_classic_fee_bps_for_entry(
        &self,
        entry: &PoolRegistryEntry,
    ) -> Result<u32> {
        let factory = entry
            .factory_address
            .unwrap_or(AERODROME_POOL_FACTORY.parse()?);
        self.fetch_aerodrome_classic_fee_bps(
            factory,
            entry.pool_address,
            entry.stable.unwrap_or(false),
        )
        .await
    }

    async fn fetch_aerodrome_classic_fee_bps(
        &self,
        factory: Address,
        pool: Address,
        stable: bool,
    ) -> Result<u32> {
        let data = encode_get_fee(pool, stable);
        let raw = self
            .eth_call(factory, &data, "Aerodrome factory getFee(address,bool)")
            .await?;
        let words = decode_32byte_words(&raw)?;
        let fee = parse_word_u256(&words[0])?;
        u32::try_from(fee).map_err(|_| anyhow::anyhow!("Aerodrome fee too large: {fee}"))
    }

    async fn fetch_aerodrome_slipstream_fee_pips_for_entry(
        &self,
        entry: &PoolRegistryEntry,
    ) -> Result<u32> {
        let factory = entry
            .factory_address
            .unwrap_or(AERODROME_SLIPSTREAM_FACTORIES[0].parse()?);
        self.fetch_aerodrome_slipstream_fee_pips(factory, entry.pool_address)
            .await
    }

    async fn fetch_aerodrome_slipstream_fee_pips(
        &self,
        factory: Address,
        pool: Address,
    ) -> Result<u32> {
        let data = encode_get_swap_fee(pool);
        let raw = self
            .eth_call(factory, &data, "Aerodrome Slipstream getSwapFee(address)")
            .await?;
        let words = decode_32byte_words(&raw)?;
        let fee = parse_word_u256(&words[0])?;
        u32::try_from(fee).map_err(|_| anyhow::anyhow!("Aerodrome Slipstream fee too large: {fee}"))
    }

    async fn fetch_aerodrome_reserves_at_block(
        &self,
        pool: Address,
        block_number: Option<u64>,
    ) -> Result<(U256, U256)> {
        let data = self
            .eth_call_at_block(pool, "0x0902f1ac", "Aerodrome getReserves()", block_number)
            .await?;
        let words = decode_32byte_words(&data)?;
        let reserve0 = parse_word_u256(&words[0])?;
        let reserve1 = parse_word_u256(&words[1])?;
        Ok((reserve0, reserve1))
    }

    async fn fetch_aerodrome_reserves_at_block_hash(
        &self,
        pool: Address,
        block_hash: &str,
    ) -> Result<(U256, U256)> {
        let data = self
            .eth_call_at_block_hash(pool, "0x0902f1ac", "Aerodrome getReserves()", block_hash)
            .await?;
        let words = decode_32byte_words(&data)?;
        let reserve0 = parse_word_u256(&words[0])?;
        let reserve1 = parse_word_u256(&words[1])?;
        Ok((reserve0, reserve1))
    }

    async fn fetch_aerodrome_slipstream_state_at_block(
        &self,
        pool: Address,
        block_number: Option<u64>,
    ) -> Result<(U256, i32, U256)> {
        let slot0 = self
            .eth_call_at_block(
                pool,
                "0x3850c7bd",
                "Aerodrome Slipstream slot0()",
                block_number,
            )
            .await?;
        let slot0_words = decode_32byte_words(&slot0)?;
        let sqrt_price_x96 = parse_word_u256(&slot0_words[0])?;
        let tick = parse_word_i24(&slot0_words[1])?;

        let liquidity = self
            .eth_call_at_block(
                pool,
                "0x1a686502",
                "Aerodrome Slipstream liquidity()",
                block_number,
            )
            .await?;
        let liquidity_words = decode_32byte_words(&liquidity)?;
        let liquidity = parse_word_u256(&liquidity_words[0])?;

        Ok((sqrt_price_x96, tick, liquidity))
    }

    async fn fetch_aerodrome_slipstream_state_at_block_hash(
        &self,
        pool: Address,
        block_hash: &str,
    ) -> Result<(U256, i32, U256)> {
        let slot0 = self
            .eth_call_at_block_hash(
                pool,
                "0x3850c7bd",
                "Aerodrome Slipstream slot0()",
                block_hash,
            )
            .await?;
        let slot0_words = decode_32byte_words(&slot0)?;
        let sqrt_price_x96 = parse_word_u256(&slot0_words[0])?;
        let tick = parse_word_i24(&slot0_words[1])?;

        let liquidity = self
            .eth_call_at_block_hash(
                pool,
                "0x1a686502",
                "Aerodrome Slipstream liquidity()",
                block_hash,
            )
            .await?;
        let liquidity_words = decode_32byte_words(&liquidity)?;
        let liquidity = parse_word_u256(&liquidity_words[0])?;

        Ok((sqrt_price_x96, tick, liquidity))
    }

    async fn fetch_uniswap_v3_state(&self, pool: Address) -> Result<(U256, i32, U256)> {
        self.fetch_uniswap_v3_state_at_block(pool, None).await
    }

    async fn fetch_uniswap_v3_state_at_block(
        &self,
        pool: Address,
        block_number: Option<u64>,
    ) -> Result<(U256, i32, U256)> {
        let slot0 = self
            .eth_call_at_block(pool, "0x3850c7bd", "UniswapV3 slot0()", block_number)
            .await?;
        let slot0_words = decode_32byte_words(&slot0)?;
        let sqrt_price_x96 = parse_word_u256(&slot0_words[0])?;
        let tick = parse_word_i24(&slot0_words[1])?;

        let liquidity = self
            .eth_call_at_block(pool, "0x1a686502", "UniswapV3 liquidity()", block_number)
            .await?;
        let liquidity_words = decode_32byte_words(&liquidity)?;
        let liquidity = parse_word_u256(&liquidity_words[0])?;

        Ok((sqrt_price_x96, tick, liquidity))
    }

    async fn fetch_uniswap_v3_state_at_block_hash(
        &self,
        pool: Address,
        block_hash: &str,
    ) -> Result<(U256, i32, U256)> {
        let slot0 = self
            .eth_call_at_block_hash(pool, "0x3850c7bd", "UniswapV3 slot0()", block_hash)
            .await?;
        let slot0_words = decode_32byte_words(&slot0)?;
        let sqrt_price_x96 = parse_word_u256(&slot0_words[0])?;
        let tick = parse_word_i24(&slot0_words[1])?;

        let liquidity = self
            .eth_call_at_block_hash(pool, "0x1a686502", "UniswapV3 liquidity()", block_hash)
            .await?;
        let liquidity_words = decode_32byte_words(&liquidity)?;
        let liquidity = parse_word_u256(&liquidity_words[0])?;

        Ok((sqrt_price_x96, tick, liquidity))
    }

    pub async fn fetch_initialized_ticks_around_state(
        &self,
        pool_state: &PoolState,
        word_radius: i32,
    ) -> Result<Vec<TickState>> {
        let Some(current_tick) = pool_state.tick else {
            return Ok(Vec::new());
        };
        if pool_state.variant == PoolVariant::AerodromeVolatile {
            return Ok(Vec::new());
        }

        let tick_spacing = self.fetch_tick_spacing(pool_state.pool_id.address).await?;
        if tick_spacing <= 0 {
            anyhow::bail!("invalid tick spacing {tick_spacing}");
        }

        let current_word = word_position(current_tick, tick_spacing);
        let block_number = self.get_block_number().await?;
        let mut ticks = Vec::new();

        for word in (current_word - word_radius)..=(current_word + word_radius) {
            let bitmap = self
                .fetch_tick_bitmap_word(pool_state.pool_id.address, word as i16)
                .await?;
            for bit in 0..256usize {
                if ((bitmap >> bit) & U256::from(1u64)).is_zero() {
                    continue;
                }
                let compressed = word
                    .checked_mul(256)
                    .and_then(|value| value.checked_add(bit as i32))
                    .ok_or_else(|| anyhow::anyhow!("compressed tick overflow"))?;
                let tick = compressed
                    .checked_mul(tick_spacing)
                    .ok_or_else(|| anyhow::anyhow!("tick overflow"))?;
                let (liquidity_gross, liquidity_net) = self
                    .fetch_tick_info(pool_state.pool_id.address, tick)
                    .await?;
                ticks.push(TickState {
                    pool_id: pool_state.pool_id.clone(),
                    tick,
                    liquidity_net,
                    liquidity_gross,
                    block_number,
                    updated_at: Utc::now(),
                });
            }
        }

        Ok(ticks)
    }

    async fn fetch_tick_spacing(&self, pool: Address) -> Result<i32> {
        let raw = self.eth_call(pool, "0xd0c93a7c", "tickSpacing()").await?;
        let words = decode_32byte_words(&raw)?;
        parse_word_i24(&words[0])
    }

    async fn fetch_tick_bitmap_word(&self, pool: Address, word_position: i16) -> Result<U256> {
        let data = format!("0x5339c296{}", encode_signed_word(word_position as i128));
        let raw = self.eth_call(pool, &data, "tickBitmap(int16)").await?;
        let words = decode_32byte_words(&raw)?;
        parse_word_u256(&words[0])
    }

    async fn fetch_tick_info(&self, pool: Address, tick: i32) -> Result<(U256, i128)> {
        let data = format!("0xf30dba93{}", encode_signed_word(tick as i128));
        let raw = self.eth_call(pool, &data, "ticks(int24)").await?;
        let words = decode_32byte_words(&raw)?;
        if words.len() < 2 {
            anyhow::bail!("ticks(int24) response too short");
        }
        Ok((parse_word_u256(&words[0])?, parse_word_i128(&words[1])?))
    }

    async fn eth_call(&self, to: Address, data: &str, label: &str) -> Result<String> {
        self.eth_call_from_at_block(None, to, data, label, None)
            .await
    }

    async fn eth_call_at_block(
        &self,
        to: Address,
        data: &str,
        label: &str,
        block_number: Option<u64>,
    ) -> Result<String> {
        self.eth_call_from_at_block(None, to, data, label, block_number)
            .await
    }

    async fn eth_call_at_block_hash(
        &self,
        to: Address,
        data: &str,
        label: &str,
        block_hash: &str,
    ) -> Result<String> {
        self.eth_call_from_at_block_hash(None, to, data, label, block_hash)
            .await
    }

    pub async fn eth_call_from(
        &self,
        from: Option<Address>,
        to: Address,
        data: &str,
        label: &str,
    ) -> Result<String> {
        self.eth_call_from_at_block(from, to, data, label, None)
            .await
    }

    pub async fn eth_call_from_at_block(
        &self,
        from: Option<Address>,
        to: Address,
        data: &str,
        label: &str,
        block_number: Option<u64>,
    ) -> Result<String> {
        let mut call = json!({
            "to": format!("{to:#x}"),
            "data": data,
        });
        if let Some(from) = from {
            call["from"] = json!(format!("{from:#x}"));
        }
        let block_tag = block_number
            .map(|block| format!("0x{block:x}"))
            .unwrap_or_else(|| "latest".to_string());
        let value = self
            .rpc("eth_call", json!([call, block_tag]))
            .await
            .with_context(|| format!("eth_call {label} to={to:#x} data={data}"))?;
        let result = value.as_str().unwrap_or("0x").to_string();
        if result == "0x" {
            anyhow::bail!("{label} returned empty result for pool {to:#x}");
        }
        Ok(result)
    }

    pub async fn eth_call_from_at_block_hash(
        &self,
        from: Option<Address>,
        to: Address,
        data: &str,
        label: &str,
        block_hash: &str,
    ) -> Result<String> {
        let mut call = json!({
            "to": format!("{to:#x}"),
            "data": data,
        });
        if let Some(from) = from {
            call["from"] = json!(format!("{from:#x}"));
        }
        let block_ref = json!({
            "blockHash": block_hash,
            "requireCanonical": true,
        });
        let value = self
            .rpc("eth_call", json!([call, block_ref]))
            .await
            .with_context(|| {
                format!("eth_call {label} to={to:#x} data={data} block_hash={block_hash}")
            })?;
        let result = value.as_str().unwrap_or("0x").to_string();
        if result == "0x" {
            anyhow::bail!("{label} returned empty result for pool {to:#x}");
        }
        Ok(result)
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let response: RpcResponse = self
            .client
            .post(&self.http_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await?
            .json()
            .await?;

        if let Some(error) = response.error {
            anyhow::bail!(
                "rpc {method} failed: code={} message={} data={}",
                error
                    .code
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                error.message,
                error
                    .data
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
        }

        response
            .result
            .ok_or_else(|| anyhow::anyhow!("rpc {method} returned no result"))
    }
}

fn slipstream_factories(settings: &Settings) -> Result<Vec<Address>> {
    let mut factories = Vec::with_capacity(AERODROME_SLIPSTREAM_FACTORIES.len() + 1);

    if let Some(factory) = settings.aerodrome_slipstream_factory {
        factories.push(factory);
    }

    for factory in AERODROME_SLIPSTREAM_FACTORIES {
        let factory = factory.parse()?;
        if !factories.contains(&factory) {
            factories.push(factory);
        }
    }

    Ok(factories)
}

#[derive(Debug, Deserialize, serde::Serialize)]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(rename = "blockNumber")]
    block_number: String,
    #[serde(rename = "transactionHash")]
    transaction_hash: String,
    #[serde(rename = "logIndex")]
    log_index: String,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub tx_hash: B256,
    pub success: bool,
    pub gas_used: Option<U256>,
    pub effective_gas_price: Option<U256>,
    pub block_number: Option<u64>,
    pub raw: Value,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: Option<i64>,
    message: String,
    data: Option<Value>,
}

fn dex_for_pool(pools: &[PoolState], pool: Address) -> DexKind {
    pools
        .iter()
        .find(|state| state.pool_id.address == pool)
        .map(|state| state.dex)
        .unwrap_or(DexKind::UniswapV3)
}

fn decode_event_type(dex: DexKind, topic0: Option<&str>) -> String {
    match (dex, topic0.unwrap_or_default()) {
        (
            DexKind::Aerodrome,
            "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1",
        ) => "Sync",
        (
            DexKind::Aerodrome,
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
        ) => "Swap",
        (
            DexKind::Aerodrome,
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
        ) => "Swap",
        (
            DexKind::Aerodrome,
            "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
        ) => "Mint",
        (
            DexKind::Aerodrome,
            "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
        ) => "Burn",
        (
            DexKind::Aerodrome,
            "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f",
        ) => "Mint",
        (
            DexKind::Aerodrome,
            "0x5d624aa9c148153ab3446c1b154f660ee7701e549fe9b62dab7171b1c80e6fa2",
        ) => "Burn",
        (
            DexKind::Aerodrome,
            "0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602",
        ) => "Fees",
        (
            DexKind::Aerodrome,
            "0x865ca08d59f5cb456e85cd2f7ef63664ea4f73327414e9d8152c4158b0e94645",
        ) => "Claim",
        (
            DexKind::Aerodrome,
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        ) => "Transfer",
        (
            DexKind::Aerodrome,
            "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
        ) => "Approval",
        (
            DexKind::Aerodrome,
            "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95",
        ) => "Initialize",
        (
            DexKind::Aerodrome,
            "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
        ) => "Collect",
        (
            DexKind::Aerodrome,
            "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633",
        ) => "Flash",
        (
            DexKind::Aerodrome,
            "0xac49e518f90a358f652e4400164f05a5d8f7e35e7747279bc3a93dbf584e125a",
        ) => "IncreaseObservationCardinalityNext",
        (
            DexKind::Aerodrome,
            "0x973d8d92bb299f4af6ce49b52a8adb85ae46b9f214c4c4fc06ac77401237b133",
        ) => "SetFeeProtocol",
        (
            DexKind::Aerodrome,
            "0x596b573906218d3411850b26a6b437d6c4522fdb43d2d2386263f86d50b8b151",
        ) => "CollectProtocol",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95",
        ) => "Initialize",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
        ) => "Swap",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
        ) => "Mint",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
        ) => "Burn",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0",
        ) => "Collect",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633",
        ) => "Flash",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0xac49e518f90a358f652e4400164f05a5d8f7e35e7747279bc3a93dbf584e125a",
        ) => "IncreaseObservationCardinalityNext",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x973d8d92bb299f4af6ce49b52a8adb85ae46b9f214c4c4fc06ac77401237b133",
        ) => "SetFeeProtocol",
        (
            DexKind::UniswapV3 | DexKind::PancakeSwap,
            "0x596b573906218d3411850b26a6b437d6c4522fdb43d2d2386263f86d50b8b151",
        ) => "CollectProtocol",
        (_, _) => "Unknown",
    }
    .to_string()
}

fn parse_hex_u64(value: &str) -> Result<u64> {
    let clean = value.trim_start_matches("0x");
    if clean.is_empty() {
        return Ok(0);
    }
    Ok(u64::from_str_radix(clean, 16)?)
}

fn parse_hex_u256(value: &str) -> Result<U256> {
    let clean = value.trim_start_matches("0x");
    if clean.is_empty() {
        return Ok(U256::ZERO);
    }
    Ok(U256::from_str_radix(clean, 16)?)
}

fn decode_32byte_words(data: &str) -> Result<Vec<String>> {
    let clean = data.trim_start_matches("0x");
    if clean.is_empty() {
        anyhow::bail!("empty eth_call result");
    }
    if clean.len() % 64 != 0 {
        anyhow::bail!("unexpected eth_call word length");
    }
    Ok((0..clean.len())
        .step_by(64)
        .map(|i| clean[i..i + 64].to_string())
        .collect())
}

fn parse_word_u256(word: &str) -> Result<U256> {
    Ok(U256::from_str_radix(word, 16)?)
}

fn parse_word_address(word: &str) -> Result<Address> {
    let address_start = word
        .len()
        .checked_sub(40)
        .ok_or_else(|| anyhow::anyhow!("abi word too short for address"))?;
    Ok(format!("0x{}", &word[address_start..]).parse()?)
}

fn parse_word_i24(word: &str) -> Result<i32> {
    let low = u32::from_str_radix(&word[word.len() - 6..], 16)?;
    let signed = if (low & 0x800000) != 0 {
        (low as i32) - (1 << 24)
    } else {
        low as i32
    };
    Ok(signed)
}

fn parse_word_i128(word: &str) -> Result<i128> {
    let low = u128::from_str_radix(&word[word.len() - 32..], 16)?;
    Ok(low as i128)
}

fn encode_signed_word(value: i128) -> String {
    let encoded = if value >= 0 {
        U256::from(value as u128)
    } else {
        U256::MAX - U256::from((-value - 1) as u128)
    };
    format!("{encoded:064x}")
}

fn word_position(tick: i32, tick_spacing: i32) -> i32 {
    tick.div_euclid(tick_spacing) >> 8
}

fn decode_single_address(data: &str) -> Result<Address> {
    let words = decode_32byte_words(data)?;
    parse_word_address(&words[0])
}

fn decode_int24_array(data: &str) -> Result<Vec<i32>> {
    let words = decode_32byte_words(data)?;
    if words.len() < 2 {
        anyhow::bail!("dynamic int24 array response is too short");
    }
    let len = parse_word_u256(&words[1])?
        .try_into()
        .map_err(|_| anyhow::anyhow!("tickSpacings() length does not fit usize"))?;
    let values = words
        .iter()
        .skip(2)
        .take(len)
        .map(|word| parse_word_i24(word))
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn decode_abi_string_or_bytes32(data: &str) -> Result<String> {
    let clean = data.trim_start_matches("0x");
    if clean.len() == 64 {
        let bytes = hex_to_bytes(clean)?;
        return Ok(trim_null_utf8(&bytes));
    }

    let words = decode_32byte_words(data)?;
    if words.len() < 2 {
        anyhow::bail!("dynamic string response is too short");
    }
    let len: usize = parse_word_u256(&words[1])?
        .try_into()
        .map_err(|_| anyhow::anyhow!("symbol() length does not fit usize"))?;
    let encoded = clean
        .get(128..)
        .ok_or_else(|| anyhow::anyhow!("dynamic string response missing data"))?;
    let byte_len = len
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("symbol() length overflow"))?;
    let symbol_hex = encoded
        .get(..byte_len)
        .ok_or_else(|| anyhow::anyhow!("dynamic string response truncated"))?;
    let bytes = hex_to_bytes(symbol_hex)?;
    Ok(trim_null_utf8(&bytes))
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    if value.len() % 2 != 0 {
        anyhow::bail!("hex string has odd length");
    }

    (0..value.len())
        .step_by(2)
        .map(|index| Ok(u8::from_str_radix(&value[index..index + 2], 16)?))
        .collect()
}

fn trim_null_utf8(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

fn encode_get_pool_bool(token_a: Address, token_b: Address, stable: bool) -> String {
    format!(
        "0x79bc57d5{}{}{}",
        encode_address_word(token_a),
        encode_address_word(token_b),
        encode_bool_word(stable),
    )
}

fn encode_get_fee(pool: Address, stable: bool) -> String {
    format!(
        "0xcc56b2c5{}{}",
        encode_address_word(pool),
        encode_bool_word(stable),
    )
}

fn encode_get_swap_fee(pool: Address) -> String {
    format!("0x35458dcc{}", encode_address_word(pool))
}

fn encode_get_pool_uint24(token_a: Address, token_b: Address, fee: u32) -> String {
    format!(
        "0x1698ee82{}{}{}",
        encode_address_word(token_a),
        encode_address_word(token_b),
        encode_u32_word(fee),
    )
}

fn encode_get_pool_int24(token_a: Address, token_b: Address, tick_spacing: i32) -> String {
    format!(
        "0x28af8d0b{}{}{}",
        encode_address_word(token_a),
        encode_address_word(token_b),
        encode_i24_word(tick_spacing),
    )
}

fn encode_address_word(address: Address) -> String {
    let clean = format!("{address:#x}").trim_start_matches("0x").to_string();
    format!("{clean:0>64}")
}

fn encode_bool_word(value: bool) -> String {
    encode_u32_word(u32::from(value))
}

fn encode_u32_word(value: u32) -> String {
    format!("{value:064x}")
}

fn encode_i24_word(value: i32) -> String {
    if value >= 0 {
        format!("{value:064x}")
    } else {
        let low24 = ((1_i32 << 24) + value) as u32;
        format!("{}{:06x}", "f".repeat(58), low24)
    }
}
