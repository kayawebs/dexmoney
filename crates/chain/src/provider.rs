use crate::events::DexEvent;
use alloy_primitives::{address, Address, U256};
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};

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
        let usdc = settings.usdc_address;
        let weth = settings.weth_address;
        let mut out = Vec::new();

        if let Some(pool) = settings.aerodrome_usdc_weth_pool {
            match self.fetch_aerodrome_reserves(pool).await {
                Ok((reserve0, reserve1)) => {
                    out.push(PoolState {
                        pool_id: PoolId {
                            chain_id: self.chain_id,
                            address: pool,
                        },
                        dex: DexKind::Aerodrome,
                        variant: PoolVariant::AerodromeVolatile,
                        token0: usdc,
                        token1: weth,
                        fee_bps: 30,
                        reserve0: Some(reserve0),
                        reserve1: Some(reserve1),
                        sqrt_price_x96: None,
                        liquidity: None,
                        tick: None,
                        block_number: self.get_block_number().await?,
                        updated_at: now,
                    });
                }
                Err(err) => {
                    warn!(
                        pool = %pool,
                        error = %err,
                        "failed to fetch Aerodrome pool reserves; skipping pool"
                    );
                }
            }
        }

        for (pool, fee_bps) in [
            (settings.uniswap_v3_usdc_weth_500_pool, 5u32),
            (settings.uniswap_v3_usdc_weth_3000_pool, 30u32),
        ] {
            if let Some(pool) = pool {
                match self.fetch_uniswap_v3_state(pool).await {
                    Ok((sqrt_price_x96, tick, liquidity)) => {
                        out.push(PoolState {
                            pool_id: PoolId {
                                chain_id: self.chain_id,
                                address: pool,
                            },
                            dex: DexKind::UniswapV3,
                            variant: PoolVariant::UniswapV3,
                            token0: weth,
                            token1: usdc,
                            fee_bps,
                            reserve0: None,
                            reserve1: None,
                            sqrt_price_x96: Some(sqrt_price_x96),
                            liquidity: Some(liquidity),
                            tick: Some(tick),
                            block_number: self.get_block_number().await?,
                            updated_at: now,
                        });
                    }
                    Err(err) => {
                        warn!(
                            pool = %pool,
                            fee_bps,
                            error = %err,
                            "failed to fetch Uniswap V3 pool state; skipping pool"
                        );
                    }
                }
            }
        }

        if out.is_empty() {
            out.push(PoolState {
                pool_id: PoolId {
                    chain_id: self.chain_id,
                    address: address!("1111111111111111111111111111111111111111"),
                },
                dex: DexKind::Aerodrome,
                variant: PoolVariant::AerodromeVolatile,
                token0: usdc,
                token1: weth,
                fee_bps: 30,
                reserve0: Some(U256::from(200_000_000_000u64)),
                reserve1: Some(U256::from(100_000_000_000_000_000_000u128)),
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                block_number: self.get_block_number().await?,
                updated_at: now,
            });
        }

        Ok(out)
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        let value = self.rpc("eth_blockNumber", json!([])).await?;
        parse_hex_u64(value.as_str().unwrap_or("0x0"))
    }

    pub async fn fetch_relevant_events(
        &self,
        settings: &Settings,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DexEvent>> {
        if from_block > to_block {
            return Ok(Vec::new());
        }

        let mut addresses = Vec::new();
        if let Some(pool) = settings.aerodrome_usdc_weth_pool {
            addresses.push(format!("{pool:#x}"));
        }
        if let Some(pool) = settings.uniswap_v3_usdc_weth_500_pool {
            addresses.push(format!("{pool:#x}"));
        }
        if let Some(pool) = settings.uniswap_v3_usdc_weth_3000_pool {
            addresses.push(format!("{pool:#x}"));
        }
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
        for log in logs {
            let raw_data_json = serde_json::to_value(&log)?;
            let pool_address: Address = log.address.parse()?;
            let dex = dex_for_pool(settings, pool_address);
            let event_type = decode_event_type(dex, log.topics.first().map(String::as_str));
            out.push(DexEvent {
                block_number: parse_hex_u64(&log.block_number)?,
                tx_hash: log.transaction_hash,
                log_index: parse_hex_u64(&log.log_index)?,
                pool_address,
                dex,
                event_type,
                raw_data_json,
            });
        }
        Ok(out)
    }

    async fn fetch_aerodrome_reserves(&self, pool: Address) -> Result<(U256, U256)> {
        let data = self
            .eth_call(pool, "0x0902f1ac", "Aerodrome getReserves()")
            .await?;
        let words = decode_32byte_words(&data)?;
        let reserve0 = parse_word_u256(&words[0])?;
        let reserve1 = parse_word_u256(&words[1])?;
        Ok((reserve0, reserve1))
    }

    async fn fetch_uniswap_v3_state(&self, pool: Address) -> Result<(U256, i32, U256)> {
        let slot0 = self
            .eth_call(pool, "0x3850c7bd", "UniswapV3 slot0()")
            .await?;
        let slot0_words = decode_32byte_words(&slot0)?;
        let sqrt_price_x96 = parse_word_u256(&slot0_words[0])?;
        let tick = parse_word_i24(&slot0_words[1])?;

        let liquidity = self
            .eth_call(pool, "0x1a686502", "UniswapV3 liquidity()")
            .await?;
        let liquidity_words = decode_32byte_words(&liquidity)?;
        let liquidity = parse_word_u256(&liquidity_words[0])?;

        Ok((sqrt_price_x96, tick, liquidity))
    }

    async fn eth_call(&self, to: Address, data: &str, label: &str) -> Result<String> {
        let value = self
            .rpc(
                "eth_call",
                json!([
                    {
                        "to": format!("{to:#x}"),
                        "data": data,
                    },
                    "latest"
                ]),
            )
            .await?;
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
            anyhow::bail!("rpc {method} failed: {}", error.message);
        }

        response
            .result
            .ok_or_else(|| anyhow::anyhow!("rpc {method} returned no result"))
    }
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

#[derive(Debug, Deserialize)]
struct RpcError {
    message: String,
}

fn dex_for_pool(settings: &Settings, pool: Address) -> DexKind {
    if settings.aerodrome_usdc_weth_pool == Some(pool) {
        DexKind::Aerodrome
    } else {
        DexKind::UniswapV3
    }
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
            DexKind::UniswapV3,
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
        ) => "Swap",
        (
            DexKind::UniswapV3,
            "0x7a53080ba414158be7ec69b987b5fb7d07dee1015d1c6ee733b3f419f0e3c2d2",
        ) => "Mint",
        (
            DexKind::UniswapV3,
            "0x0c396cd989a39f4459b5fa1aed6a9a8e9d0dc76f0f6d4c1d3c2f3f6721e5d2fb",
        ) => "Burn",
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

fn parse_word_i24(word: &str) -> Result<i32> {
    let low = u32::from_str_radix(&word[word.len() - 6..], 16)?;
    let signed = if (low & 0x800000) != 0 {
        (low as i32) - (1 << 24)
    } else {
        low as i32
    };
    Ok(signed)
}
