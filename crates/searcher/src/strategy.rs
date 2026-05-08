use alloy_primitives::{address, Address, U256};
use chrono::Utc;

use base_arb_common::config::Settings;
use base_arb_common::types::{
    ArbPath, Candidate, DexKind, PoolId, PoolState, PoolVariant, QuoteResult, SwapStep,
};
use base_arb_dex::aerodrome::AerodromeVolatileQuoter;
use base_arb_dex::quoter::DexQuoter;
use base_arb_dex::uniswap_v3::UniswapV3ChainQuoter;

use crate::opportunity::build_candidate;

pub struct SearchEngine {
    pub amount_sizes: Vec<U256>,
    pub paths: Vec<ArbPath>,
    pub min_expected_profit: U256,
    pub max_price_impact_bps: u64,
    pub whitelist_paths: Vec<String>,
    pub candidate_ttl_ms: i64,
}

impl SearchEngine {
    pub fn new(
        candidate_ttl_ms: i64,
        max_price_impact_bps: u64,
        min_expected_profit: U256,
    ) -> Self {
        let usdc = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let weth = address!("4200000000000000000000000000000000000006");
        let aero_pool = address!("1111111111111111111111111111111111111111");
        let uni_pool = address!("2222222222222222222222222222222222222222");
        let name1 = "usdc-weth-usdc-aero-uni".to_string();
        let name2 = "usdc-weth-usdc-uni-aero".to_string();

        Self {
            amount_sizes: vec![
                U256::from(10_000_000u64),
                U256::from(30_000_000u64),
                U256::from(50_000_000u64),
                U256::from(100_000_000u64),
            ],
            paths: vec![
                ArbPath {
                    name: name1.clone(),
                    steps: vec![
                        SwapStep {
                            dex: DexKind::Aerodrome,
                            pool: aero_pool,
                            token_in: usdc,
                            token_out: weth,
                            fee_bps: Some(30),
                        },
                        SwapStep {
                            dex: DexKind::UniswapV3,
                            pool: uni_pool,
                            token_in: weth,
                            token_out: usdc,
                            fee_bps: Some(30),
                        },
                    ],
                },
                ArbPath {
                    name: name2.clone(),
                    steps: vec![
                        SwapStep {
                            dex: DexKind::UniswapV3,
                            pool: uni_pool,
                            token_in: usdc,
                            token_out: weth,
                            fee_bps: Some(30),
                        },
                        SwapStep {
                            dex: DexKind::Aerodrome,
                            pool: aero_pool,
                            token_in: weth,
                            token_out: usdc,
                            fee_bps: Some(30),
                        },
                    ],
                },
            ],
            min_expected_profit,
            max_price_impact_bps,
            whitelist_paths: vec![name1, name2],
            candidate_ttl_ms,
        }
    }

    pub fn search(&self, pool_states: &[PoolState]) -> anyhow::Result<Vec<Candidate>> {
        let usdc = self.paths[0].steps[0].token_in;
        let mut out = Vec::new();

        for path in &self.paths {
            for amount_in in &self.amount_sizes {
                if let Some((expected_amount_out, price_impact_bps)) =
                    quote_path(pool_states, path, *amount_in)?
                {
                    if price_impact_bps > self.max_price_impact_bps {
                        continue;
                    }
                    let expected_profit = expected_amount_out.saturating_sub(*amount_in);
                    let candidate = build_candidate(
                        1,
                        "mvp-usdc-weth-usdc".into(),
                        usdc,
                        *amount_in,
                        expected_amount_out,
                        expected_profit,
                        self.min_expected_profit,
                        price_impact_bps,
                        path.clone(),
                        self.candidate_ttl_ms,
                    );
                    out.push(candidate);
                }
            }
        }

        Ok(out)
    }
}

pub fn usdc_to_units(usdc: f64) -> U256 {
    U256::from((usdc * 1_000_000.0) as u64)
}

pub fn engine_from_settings(
    settings: &Settings,
    candidate_ttl_ms: i64,
    max_price_impact_bps: u64,
    min_expected_profit: U256,
) -> anyhow::Result<SearchEngine> {
    let Some(aero_pool) = settings.aerodrome_usdc_weth_pool else {
        anyhow::bail!("AERODROME_USDC_WETH_POOL is required");
    };

    let mut paths = Vec::new();
    let mut whitelist_paths = Vec::new();

    for (pool, label, fee_bps) in [
        (settings.uniswap_v3_usdc_weth_500_pool, "uni500", 5u32),
        (settings.uniswap_v3_usdc_weth_3000_pool, "uni3000", 30u32),
    ] {
        let Some(uni_pool) = pool else {
            continue;
        };

        let forward_name = format!("usdc-weth-usdc-aero-{label}");
        let reverse_name = format!("usdc-weth-usdc-{label}-aero");

        whitelist_paths.push(forward_name.clone());
        whitelist_paths.push(reverse_name.clone());

        paths.push(ArbPath {
            name: forward_name,
            steps: vec![
                SwapStep {
                    dex: DexKind::Aerodrome,
                    pool: aero_pool,
                    token_in: settings.usdc_address,
                    token_out: settings.weth_address,
                    fee_bps: Some(30),
                },
                SwapStep {
                    dex: DexKind::UniswapV3,
                    pool: uni_pool,
                    token_in: settings.weth_address,
                    token_out: settings.usdc_address,
                    fee_bps: Some(fee_bps),
                },
            ],
        });

        paths.push(ArbPath {
            name: reverse_name,
            steps: vec![
                SwapStep {
                    dex: DexKind::UniswapV3,
                    pool: uni_pool,
                    token_in: settings.usdc_address,
                    token_out: settings.weth_address,
                    fee_bps: Some(fee_bps),
                },
                SwapStep {
                    dex: DexKind::Aerodrome,
                    pool: aero_pool,
                    token_in: settings.weth_address,
                    token_out: settings.usdc_address,
                    fee_bps: Some(30),
                },
            ],
        });
    }

    if paths.is_empty() {
        anyhow::bail!(
            "at least one of UNISWAP_V3_USDC_WETH_500_POOL or UNISWAP_V3_USDC_WETH_3000_POOL is required"
        );
    }

    Ok(SearchEngine {
        amount_sizes: vec![
            U256::from(10_000_000u64),
            U256::from(30_000_000u64),
            U256::from(50_000_000u64),
            U256::from(100_000_000u64),
        ],
        paths,
        min_expected_profit,
        max_price_impact_bps,
        whitelist_paths,
        candidate_ttl_ms,
    })
}

pub fn demo_pool_states(usdc: Address) -> Vec<PoolState> {
    let weth = address!("4200000000000000000000000000000000000006");
    let aero_pool = address!("1111111111111111111111111111111111111111");
    let uni_pool = address!("2222222222222222222222222222222222222222");
    let now = Utc::now();

    vec![
        PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: aero_pool,
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
        },
        PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: uni_pool,
            },
            dex: DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            token0: weth,
            token1: usdc,
            fee_bps: 30,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(79_228_162_514_264_337_593_543_950_336u128)),
            liquidity: Some(U256::from(1_000_000_000u64)),
            tick: Some(0),
            block_number: 1,
            updated_at: now,
        },
    ]
}

fn quote_path(
    pool_states: &[PoolState],
    path: &ArbPath,
    amount_in: U256,
) -> anyhow::Result<Option<(U256, u64)>> {
    let aero = AerodromeVolatileQuoter;
    let uni = UniswapV3ChainQuoter;
    let mut amount = amount_in;
    let mut max_impact = 0u64;

    for step in &path.steps {
        let pool_state = match pool_states
            .iter()
            .find(|state| state.pool_id.address == step.pool)
        {
            Some(state) => state,
            None => return Ok(None),
        };

        let quote = match step.dex {
            DexKind::Aerodrome => {
                futures::executor::block_on(aero.quote_exact_in(pool_state, step.token_in, amount))
                    .map_err(anyhow::Error::from)?
            }
            DexKind::UniswapV3 => {
                futures::executor::block_on(uni.quote_exact_in(pool_state, step.token_in, amount))
                    .map_err(anyhow::Error::from)?
            }
        };

        amount = quote.amount_out;
        max_impact = max_impact.max(estimate_price_impact_bps(pool_state, &quote));
    }

    Ok(Some((amount, max_impact)))
}

fn estimate_price_impact_bps(pool_state: &PoolState, quote: &QuoteResult) -> u64 {
    if let (Some(reserve0), Some(reserve1)) = (pool_state.reserve0, pool_state.reserve1) {
        let reserve_in = reserve0.max(U256::from(1u64));
        let reserve_out = reserve1.max(U256::from(1u64));
        let spot_out = quote
            .amount_in
            .saturating_mul(reserve_out)
            .checked_div(reserve_in)
            .unwrap_or(U256::ZERO);
        if spot_out.is_zero() {
            return 0;
        }
        let slippage = spot_out.saturating_sub(quote.amount_out);
        let bps = slippage
            .saturating_mul(U256::from(10_000u64))
            .checked_div(spot_out)
            .unwrap_or(U256::ZERO);
        return u64::try_from(bps).unwrap_or(u64::MAX);
    }

    5
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};

    use super::{demo_pool_states, SearchEngine};

    #[test]
    fn search_engine_emits_candidates_for_demo_state() {
        let engine = SearchEngine::new(500, 50, U256::from(1u64));
        let candidates = engine
            .search(&demo_pool_states(address!(
                "833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
            )))
            .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|c| !c.path.steps.is_empty()));
    }
}
