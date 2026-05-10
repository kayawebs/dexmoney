use alloy_primitives::{address, Address, U256};
use chrono::Utc;

use base_arb_common::config::Settings;
use base_arb_common::types::{
    ArbPath, Candidate, DexKind, PoolId, PoolState, PoolVariant, QuoteDiagnostics, QuoteResult,
    SwapStep, TickState,
};
use base_arb_dex::aerodrome::AerodromeVolatileQuoter;
use base_arb_dex::quoter::DexQuoter;
use base_arb_dex::uniswap_v3::{
    quote_exact_in_with_ticks_diagnostics, spot_quote_exact_in, UniswapV3CurrentTickQuoter,
};

use crate::opportunity::build_candidate;

pub struct SearchEngine {
    pub amount_sizes: Vec<U256>,
    pub paths: Vec<ArbPath>,
    pub min_expected_profit: U256,
    pub max_price_impact_bps: u64,
    pub whitelist_paths: Vec<String>,
    pub candidate_ttl_ms: i64,
    pub usdc: Address,
    pub weth: Address,
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
                    diagnostics: None,
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
                    diagnostics: None,
                },
            ],
            min_expected_profit,
            max_price_impact_bps,
            whitelist_paths: vec![name1, name2],
            candidate_ttl_ms,
            usdc,
            weth,
        }
    }

    pub async fn search(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
    ) -> anyhow::Result<Vec<Candidate>> {
        let paths = self.paths_for_pool_states(pool_states);
        let mut out = Vec::new();

        for path in &paths {
            for amount_in in &self.amount_sizes {
                if let Some((expected_amount_out, price_impact_bps, diagnostics)) =
                    quote_path(pool_states, tick_states, path, *amount_in).await?
                {
                    if price_impact_bps > self.max_price_impact_bps {
                        continue;
                    }
                    let expected_profit = expected_amount_out.saturating_sub(*amount_in);
                    let block_number = path
                        .steps
                        .iter()
                        .filter_map(|step| {
                            pool_states
                                .iter()
                                .find(|state| state.pool_id.address == step.pool)
                                .map(|state| state.block_number)
                        })
                        .max()
                        .unwrap_or(0);
                    let candidate = build_candidate(
                        block_number,
                        "mvp-usdc-weth-usdc".into(),
                        self.usdc,
                        *amount_in,
                        expected_amount_out,
                        expected_profit,
                        self.min_expected_profit,
                        price_impact_bps,
                        path_with_diagnostics(path, diagnostics),
                        self.candidate_ttl_ms,
                    );
                    out.push(candidate);
                }
            }
        }

        Ok(out)
    }

    fn paths_for_pool_states(&self, pool_states: &[PoolState]) -> Vec<ArbPath> {
        if !self.paths.is_empty() {
            return self.paths.clone();
        }

        let pools = pool_states
            .iter()
            .filter(|state| is_supported_usdc_weth_pool(state, self.usdc, self.weth))
            .collect::<Vec<_>>();
        let mut paths = Vec::new();

        for first in &pools {
            for second in &pools {
                if first.pool_id.address == second.pool_id.address {
                    continue;
                }
                if first.dex == second.dex && first.variant == second.variant {
                    continue;
                }

                let name = format!(
                    "usdc-weth-usdc-{}-{}",
                    pool_label(first),
                    pool_label(second)
                );
                paths.push(ArbPath {
                    name,
                    steps: vec![
                        SwapStep {
                            dex: first.dex,
                            pool: first.pool_id.address,
                            token_in: self.usdc,
                            token_out: self.weth,
                            fee_bps: Some(first.fee_bps),
                        },
                        SwapStep {
                            dex: second.dex,
                            pool: second.pool_id.address,
                            token_in: self.weth,
                            token_out: self.usdc,
                            fee_bps: Some(second.fee_bps),
                        },
                    ],
                    diagnostics: None,
                });
            }
        }

        paths
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
    Ok(SearchEngine {
        amount_sizes: vec![
            U256::from(10_000_000u64),
            U256::from(30_000_000u64),
            U256::from(50_000_000u64),
            U256::from(100_000_000u64),
        ],
        paths: Vec::new(),
        min_expected_profit,
        max_price_impact_bps,
        whitelist_paths: Vec::new(),
        candidate_ttl_ms,
        usdc: settings.usdc_address,
        weth: settings.weth_address,
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

async fn quote_path(
    pool_states: &[PoolState],
    tick_states: &[TickState],
    path: &ArbPath,
    amount_in: U256,
) -> anyhow::Result<Option<(U256, u64, QuoteDiagnostics)>> {
    let aero = AerodromeVolatileQuoter;
    let uni = UniswapV3CurrentTickQuoter;
    let mut amount = amount_in;
    let mut max_impact = 0u64;
    let mut diagnostics = QuoteDiagnostics {
        modes: Vec::new(),
        ticks_used: 0,
        crossed_ticks: 0,
        tick_range_exhausted: false,
        v3_pools_without_ticks: 0,
    };

    for step in &path.steps {
        let pool_state = match pool_states
            .iter()
            .find(|state| state.pool_id.address == step.pool)
        {
            Some(state) => state,
            None => return Ok(None),
        };

        let quote = match pool_state.variant {
            PoolVariant::AerodromeVolatile => aero
                .quote_exact_in(pool_state, step.token_in, amount)
                .await
                .map(|quote| {
                    diagnostics.modes.push("classic_reserve".into());
                    quote
                })
                .map_err(anyhow::Error::from)?,
            PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 => {
                let pool_ticks = tick_states
                    .iter()
                    .filter(|tick| tick.pool_id.address == pool_state.pool_id.address)
                    .cloned()
                    .collect::<Vec<_>>();
                if pool_ticks.is_empty() {
                    diagnostics.modes.push("v3_current_tick_fallback".into());
                    diagnostics.v3_pools_without_ticks += 1;
                    uni.quote_exact_in(pool_state, step.token_in, amount)
                        .await
                        .map_err(anyhow::Error::from)?
                } else {
                    let (quote, v3_diagnostics) = quote_exact_in_with_ticks_diagnostics(
                        pool_state,
                        &pool_ticks,
                        step.token_in,
                        amount,
                    )
                    .map_err(anyhow::Error::from)?;
                    diagnostics.modes.push("v3_cross_tick".into());
                    diagnostics.ticks_used += v3_diagnostics.ticks_used;
                    diagnostics.crossed_ticks += v3_diagnostics.crossed_ticks;
                    diagnostics.tick_range_exhausted |= v3_diagnostics.tick_range_exhausted;
                    quote
                }
            }
        };

        amount = quote.amount_out;
        max_impact = max_impact.max(estimate_price_impact_bps(pool_state, step.token_in, &quote));
    }

    Ok(Some((amount, max_impact, diagnostics)))
}

fn path_with_diagnostics(path: &ArbPath, diagnostics: QuoteDiagnostics) -> ArbPath {
    let mut path = path.clone();
    path.diagnostics = Some(diagnostics);
    path
}

fn estimate_price_impact_bps(
    pool_state: &PoolState,
    token_in: Address,
    quote: &QuoteResult,
) -> u64 {
    if let (Some(reserve0), Some(reserve1)) = (pool_state.reserve0, pool_state.reserve1) {
        let (reserve_in, reserve_out) = if token_in == pool_state.token0 {
            (reserve0, reserve1)
        } else if token_in == pool_state.token1 {
            (reserve1, reserve0)
        } else {
            return u64::MAX;
        };
        let spot_out = quote
            .amount_in
            .saturating_mul(reserve_out.max(U256::from(1u64)))
            .checked_div(reserve_in.max(U256::from(1u64)))
            .unwrap_or(U256::ZERO);
        return impact_from_spot(spot_out, quote.amount_out);
    }

    let Ok(spot_out) = spot_quote_exact_in(pool_state, token_in, quote.amount_in) else {
        return u64::MAX;
    };
    impact_from_spot(spot_out, quote.amount_out)
}

fn impact_from_spot(spot_out: U256, actual_out: U256) -> u64 {
    if spot_out.is_zero() {
        return 0;
    }
    let slippage = spot_out.saturating_sub(actual_out);
    let bps = slippage
        .saturating_mul(U256::from(10_000u64))
        .checked_div(spot_out)
        .unwrap_or(U256::ZERO);
    u64::try_from(bps).unwrap_or(u64::MAX)
}

fn is_supported_usdc_weth_pool(state: &PoolState, usdc: Address, weth: Address) -> bool {
    let has_pair = (state.token0 == usdc && state.token1 == weth)
        || (state.token0 == weth && state.token1 == usdc);
    if !has_pair {
        return false;
    }
    match state.variant {
        PoolVariant::AerodromeVolatile => state.reserve0.is_some() && state.reserve1.is_some(),
        PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 => {
            state.sqrt_price_x96.is_some() && state.liquidity.is_some() && state.tick.is_some()
        }
    }
}

fn pool_label(state: &PoolState) -> String {
    let suffix = format!("{:#x}", state.pool_id.address);
    let suffix = &suffix[suffix.len().saturating_sub(6)..];
    match (state.dex, state.variant) {
        (DexKind::Aerodrome, PoolVariant::AerodromeVolatile) => format!("aero-classic-{suffix}"),
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream) => {
            format!("aero-slipstream-{suffix}")
        }
        (DexKind::UniswapV3, PoolVariant::UniswapV3) => format!("uni-v3-{suffix}"),
        _ => format!("pool-{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};

    use super::{demo_pool_states, SearchEngine};

    #[tokio::test]
    async fn search_engine_emits_candidates_for_demo_state() {
        let engine = SearchEngine::new(500, 10_000, U256::from(1u64));
        let candidates = engine
            .search(
                &demo_pool_states(address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913")),
                &[],
            )
            .await
            .unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|c| !c.path.steps.is_empty()));
    }
}
