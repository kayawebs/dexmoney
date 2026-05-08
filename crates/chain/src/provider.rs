use alloy_primitives::{address, U256};
use anyhow::Result;
use base_arb_common::config::Settings;
use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};
use chrono::Utc;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ChainProvider {
    pub http_url: String,
    pub ws_url: String,
    pub chain_id: u64,
}

impl ChainProvider {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            http_url: settings.base_rpc_http.clone(),
            ws_url: settings.base_rpc_ws.clone(),
            chain_id: settings.chain_id,
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
                reserve0: Some(U256::from(200_000_000_000u64)),
                reserve1: Some(U256::from(100_000_000_000_000_000_000u128)),
                sqrt_price_x96: None,
                liquidity: None,
                tick: None,
                block_number: 1,
                updated_at: now,
            });
        }

        for (pool, fee_bps) in [
            (settings.uniswap_v3_usdc_weth_500_pool, 5u32),
            (settings.uniswap_v3_usdc_weth_3000_pool, 30u32),
        ] {
            if let Some(pool) = pool {
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
                    sqrt_price_x96: Some(U256::from(79_228_162_514_264_337_593_543_950_336u128)),
                    liquidity: Some(U256::from(1_000_000_000u64)),
                    tick: Some(0),
                    block_number: 1,
                    updated_at: now,
                });
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
                block_number: 1,
                updated_at: now,
            });
        }

        Ok(out)
    }
}
