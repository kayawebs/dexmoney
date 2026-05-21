use alloy_primitives::{Address, U256};
use std::collections::HashMap;

use base_arb_common::config::Settings;
use base_arb_common::types::{
    ArbPath, Candidate, DexKind, PoolState, PoolVariant, QuoteDiagnostics, QuoteResult, SwapStep,
    TickState, TokenPairSearchConfig,
};
use base_arb_dex::aerodrome::{AerodromeStableQuoter, AerodromeVolatileQuoter};
use base_arb_dex::quoter::DexQuoter;
use base_arb_dex::uniswap_v3::{
    quote_exact_in_with_ticks_diagnostics, spot_quote_exact_in, UniswapV3CurrentTickQuoter,
};
use tracing::debug;

use crate::opportunity::build_candidate;

pub struct SearchEngine {
    pub amount_sizes: Vec<U256>,
    pub paths: Vec<ArbPath>,
    pub pair_configs: Vec<TokenPairSearchConfig>,
    pub min_expected_profit: U256,
    pub max_price_impact_bps: u64,
    pub whitelist_paths: Vec<String>,
    pub candidate_ttl_ms: i64,
    pub v3_quote_safety_bps: u64,
}

#[derive(Debug, Clone, Default)]
pub struct SearchStats {
    pub paths: usize,
    pub quote_attempts: u64,
    pub quote_successes: u64,
    pub quote_skipped: u64,
    pub price_impact_rejected: u64,
    pub candidates_emitted: u64,
    pub best_profit: U256,
}

impl SearchStats {
    pub fn merge(&mut self, other: &SearchStats) {
        self.paths += other.paths;
        self.quote_attempts += other.quote_attempts;
        self.quote_successes += other.quote_successes;
        self.quote_skipped += other.quote_skipped;
        self.price_impact_rejected += other.price_impact_rejected;
        self.candidates_emitted += other.candidates_emitted;
        self.best_profit = self.best_profit.max(other.best_profit);
    }
}

impl SearchEngine {
    #[cfg(test)]
    pub fn new(
        candidate_ttl_ms: i64,
        max_price_impact_bps: u64,
        min_expected_profit: U256,
    ) -> Self {
        use alloy_primitives::address;

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
                            variant: Some(PoolVariant::AerodromeVolatile),
                            factory_address: None,
                            pool: aero_pool,
                            token_in: usdc,
                            token_out: weth,
                            fee_bps: Some(30),
                            stable: Some(false),
                            tick_spacing: None,
                        },
                        SwapStep {
                            dex: DexKind::UniswapV3,
                            variant: Some(PoolVariant::UniswapV3),
                            factory_address: None,
                            pool: uni_pool,
                            token_in: weth,
                            token_out: usdc,
                            fee_bps: Some(30),
                            stable: None,
                            tick_spacing: None,
                        },
                    ],
                    diagnostics: None,
                },
                ArbPath {
                    name: name2.clone(),
                    steps: vec![
                        SwapStep {
                            dex: DexKind::UniswapV3,
                            variant: Some(PoolVariant::UniswapV3),
                            factory_address: None,
                            pool: uni_pool,
                            token_in: usdc,
                            token_out: weth,
                            fee_bps: Some(30),
                            stable: None,
                            tick_spacing: None,
                        },
                        SwapStep {
                            dex: DexKind::Aerodrome,
                            variant: Some(PoolVariant::AerodromeVolatile),
                            factory_address: None,
                            pool: aero_pool,
                            token_in: weth,
                            token_out: usdc,
                            fee_bps: Some(30),
                            stable: Some(false),
                            tick_spacing: None,
                        },
                    ],
                    diagnostics: None,
                },
            ],
            pair_configs: Vec::new(),
            min_expected_profit,
            max_price_impact_bps,
            whitelist_paths: vec![name1, name2],
            candidate_ttl_ms,
            v3_quote_safety_bps: 0,
        }
    }

    pub async fn search(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
    ) -> anyhow::Result<Vec<Candidate>> {
        Ok(self.search_with_stats(pool_states, tick_states).await?.0)
    }

    pub async fn search_with_stats(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
    ) -> anyhow::Result<(Vec<Candidate>, SearchStats)> {
        let paths = self.paths_for_pool_states(pool_states);
        let mut stats = SearchStats {
            paths: paths.len(),
            ..SearchStats::default()
        };
        let mut out = Vec::new();

        for search_path in &paths {
            for amount_in in &search_path.amount_sizes {
                stats.quote_attempts += 1;
                let quote = match quote_path(
                    pool_states,
                    tick_states,
                    &search_path.path,
                    *amount_in,
                    self.v3_quote_safety_bps,
                )
                .await
                {
                    Ok(Some(quote)) => {
                        stats.quote_successes += 1;
                        quote
                    }
                    Ok(None) => {
                        stats.quote_skipped += 1;
                        continue;
                    }
                    Err(err) => {
                        stats.quote_skipped += 1;
                        debug!(
                            path = %search_path.path.name,
                            amount_in = %amount_in,
                            error = %err,
                            "quote skipped"
                        );
                        continue;
                    }
                };
                let (expected_amount_out, price_impact_bps, diagnostics) = quote;
                {
                    if price_impact_bps > self.max_price_impact_bps {
                        stats.price_impact_rejected += 1;
                        continue;
                    }
                    let expected_profit = expected_amount_out.saturating_sub(*amount_in);
                    stats.best_profit = stats.best_profit.max(expected_profit);
                    let block_number = search_path
                        .path
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
                        "pair-two-pool-cycle".into(),
                        search_path.path.steps[0].token_in,
                        *amount_in,
                        expected_amount_out,
                        expected_profit,
                        self.min_expected_profit,
                        price_impact_bps,
                        path_with_diagnostics(&search_path.path, diagnostics),
                        self.candidate_ttl_ms,
                    );
                    let candidate = Candidate {
                        min_profit: search_path.min_profit,
                        ..candidate
                    };
                    stats.candidates_emitted += 1;
                    out.push(candidate);
                }
            }
        }

        Ok((out, stats))
    }

    fn paths_for_pool_states(&self, pool_states: &[PoolState]) -> Vec<SearchPath> {
        if !self.paths.is_empty() {
            return self
                .paths
                .iter()
                .cloned()
                .map(|path| SearchPath {
                    path,
                    amount_sizes: self.amount_sizes.clone(),
                    min_profit: self.min_expected_profit,
                })
                .collect();
        }

        let mut paths = Vec::new();
        let configs = self
            .pair_configs
            .iter()
            .map(|config| ((config.token0, config.token1), config))
            .collect::<HashMap<_, _>>();

        for config in configs.values() {
            let pools = pool_states
                .iter()
                .filter(|state| is_supported_config_pool(state, config))
                .collect::<Vec<_>>();
            for first in &pools {
                for second in &pools {
                    if first.pool_id.address == second.pool_id.address {
                        continue;
                    }
                    add_pair_direction_paths(&mut paths, config, first, second);
                }
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
    pair_configs: Vec<TokenPairSearchConfig>,
) -> anyhow::Result<SearchEngine> {
    Ok(SearchEngine {
        amount_sizes: parse_search_amounts(settings.search_amount_usdc.as_deref())?,
        paths: Vec::new(),
        pair_configs,
        min_expected_profit,
        max_price_impact_bps,
        whitelist_paths: Vec::new(),
        candidate_ttl_ms,
        v3_quote_safety_bps: settings.v3_quote_safety_bps,
    })
}

#[derive(Clone)]
struct SearchPath {
    path: ArbPath,
    amount_sizes: Vec<U256>,
    min_profit: U256,
}

fn parse_search_amounts(raw: Option<&str>) -> anyhow::Result<Vec<U256>> {
    let raw = raw.unwrap_or("10,30,50,100");
    let mut out = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: f64 = trimmed
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid SEARCH_AMOUNT_USDC value: {trimmed}"))?;
        if !value.is_finite() || value <= 0.0 {
            anyhow::bail!("SEARCH_AMOUNT_USDC values must be positive: {trimmed}");
        }
        out.push(usdc_to_units(value));
    }
    if out.is_empty() {
        anyhow::bail!("SEARCH_AMOUNT_USDC must contain at least one amount");
    }
    Ok(out)
}

#[cfg(test)]
pub fn demo_pool_states(usdc: Address) -> Vec<PoolState> {
    use alloy_primitives::address;
    use base_arb_common::types::PoolId;
    use chrono::Utc;

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
            factory_address: None,
            token0: usdc,
            token1: weth,
            token0_decimals: Some(6),
            token1_decimals: Some(18),
            fee_bps: 30,
            stable: Some(false),
            reserve0: Some(U256::from(200_000_000_000u64)),
            reserve1: Some(U256::from(100_000_000_000_000_000_000u128)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
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
            factory_address: None,
            token0: weth,
            token1: usdc,
            token0_decimals: Some(18),
            token1_decimals: Some(6),
            fee_bps: 30,
            stable: None,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(79_228_162_514_264_337_593_543_950_336u128)),
            liquidity: Some(U256::from(1_000_000_000u64)),
            tick: Some(0),
            tick_spacing: None,
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
    v3_quote_safety_bps: u64,
) -> anyhow::Result<Option<(U256, u64, QuoteDiagnostics)>> {
    let aero_stable = AerodromeStableQuoter;
    let aero_volatile = AerodromeVolatileQuoter;
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

        let mut quote = match pool_state.variant {
            PoolVariant::AerodromeVolatile => {
                if pool_state.stable.unwrap_or(false) {
                    aero_stable
                        .quote_exact_in(pool_state, step.token_in, amount)
                        .await
                        .map(|quote| {
                            diagnostics.modes.push("classic_stable".into());
                            quote
                        })
                        .map_err(anyhow::Error::from)?
                } else {
                    aero_volatile
                        .quote_exact_in(pool_state, step.token_in, amount)
                        .await
                        .map(|quote| {
                            diagnostics.modes.push("classic_volatile".into());
                            quote
                        })
                        .map_err(anyhow::Error::from)?
                }
            }
            PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
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
        if is_v3_style_variant(pool_state.variant) && v3_quote_safety_bps > 0 {
            quote.amount_out = apply_quote_haircut(quote.amount_out, v3_quote_safety_bps)?;
        }

        amount = quote.amount_out;
        max_impact = max_impact.max(estimate_price_impact_bps(pool_state, step.token_in, &quote));
    }

    if diagnostics.v3_pools_without_ticks > 0 {
        debug!(
            path = %path.name,
            missing_v3_tick_pools = diagnostics.v3_pools_without_ticks,
            "quote skipped: V3 initialized tick data unavailable"
        );
        return Ok(None);
    }
    if diagnostics.tick_range_exhausted {
        debug!(
            path = %path.name,
            ticks_used = diagnostics.ticks_used,
            crossed_ticks = diagnostics.crossed_ticks,
            "quote skipped: V3 quote exhausted known tick range"
        );
        return Ok(None);
    }

    Ok(Some((amount, max_impact, diagnostics)))
}

fn path_with_diagnostics(path: &ArbPath, diagnostics: QuoteDiagnostics) -> ArbPath {
    let mut path = path.clone();
    path.diagnostics = Some(diagnostics);
    path
}

fn apply_quote_haircut(amount: U256, haircut_bps: u64) -> anyhow::Result<U256> {
    if haircut_bps == 0 || amount.is_zero() {
        return Ok(amount);
    }
    let denominator = U256::from(10_000u64);
    let numerator = denominator
        .checked_sub(U256::from(haircut_bps.min(10_000)))
        .ok_or_else(|| anyhow::anyhow!("invalid V3 quote haircut"))?;
    amount
        .checked_mul(numerator)
        .and_then(|value| value.checked_div(denominator))
        .ok_or_else(|| anyhow::anyhow!("V3 quote haircut overflow"))
}

fn is_v3_style_variant(variant: PoolVariant) -> bool {
    matches!(
        variant,
        PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3
    )
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

fn add_pair_direction_paths(
    paths: &mut Vec<SearchPath>,
    config: &TokenPairSearchConfig,
    first: &PoolState,
    second: &PoolState,
) {
    if !config.token0_search_amounts.is_empty() {
        paths.push(build_search_path(
            config,
            first,
            second,
            config.token0,
            config.token1,
            config.token0_search_amounts.clone(),
            config.token0_min_profit,
        ));
    }

    if !config.token1_search_amounts.is_empty() {
        paths.push(build_search_path(
            config,
            first,
            second,
            config.token1,
            config.token0,
            config.token1_search_amounts.clone(),
            config.token1_min_profit,
        ));
    }
}

fn build_search_path(
    config: &TokenPairSearchConfig,
    first: &PoolState,
    second: &PoolState,
    token_in: Address,
    token_mid: Address,
    amount_sizes: Vec<U256>,
    min_profit: U256,
) -> SearchPath {
    let name = format!(
        "{}-{}-{}-{}-{}",
        config.symbol,
        short_token(token_in),
        short_token(token_mid),
        pool_label(first),
        pool_label(second)
    );
    SearchPath {
        path: ArbPath {
            name,
            steps: vec![
                SwapStep {
                    dex: first.dex,
                    variant: Some(first.variant),
                    factory_address: first.factory_address,
                    pool: first.pool_id.address,
                    token_in,
                    token_out: token_mid,
                    fee_bps: Some(first.fee_bps),
                    stable: first.stable,
                    tick_spacing: first.tick_spacing,
                },
                SwapStep {
                    dex: second.dex,
                    variant: Some(second.variant),
                    factory_address: second.factory_address,
                    pool: second.pool_id.address,
                    token_in: token_mid,
                    token_out: token_in,
                    fee_bps: Some(second.fee_bps),
                    stable: second.stable,
                    tick_spacing: second.tick_spacing,
                },
            ],
            diagnostics: None,
        },
        amount_sizes,
        min_profit,
    }
}

fn is_supported_config_pool(state: &PoolState, config: &TokenPairSearchConfig) -> bool {
    let has_pair = (state.token0 == config.token0 && state.token1 == config.token1)
        || (state.token0 == config.token1 && state.token1 == config.token0);
    if !has_pair {
        return false;
    }
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            if state.stable.unwrap_or(false)
                && (state.token0_decimals.is_none() || state.token1_decimals.is_none())
            {
                return false;
            }
            state.reserve0.is_some() && state.reserve1.is_some()
        }
        PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
            match (state.sqrt_price_x96, state.liquidity, state.tick) {
                (Some(sqrt_price_x96), Some(liquidity), Some(_)) => {
                    !sqrt_price_x96.is_zero() && !liquidity.is_zero()
                }
                _ => false,
            }
        }
        PoolVariant::AerodromeSlipstream => {
            match (
                state.sqrt_price_x96,
                state.liquidity,
                state.tick,
                state.tick_spacing,
            ) {
                (Some(sqrt_price_x96), Some(liquidity), Some(_), Some(tick_spacing)) => {
                    tick_spacing > 0 && !sqrt_price_x96.is_zero() && !liquidity.is_zero()
                }
                _ => false,
            }
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
        (DexKind::PancakeSwap, PoolVariant::PancakeV3) => format!("pancake-v3-{suffix}"),
        _ => format!("pool-{suffix}"),
    }
}

fn short_token(token: Address) -> String {
    let token = format!("{token:#x}");
    token[token.len().saturating_sub(6)..].to_string()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use base_arb_common::types::{PoolId, TickState, TokenPairSearchConfig};
    use chrono::Utc;

    use super::{demo_pool_states, is_supported_config_pool, SearchEngine};

    #[tokio::test]
    async fn search_engine_emits_candidates_for_demo_state() {
        let engine = SearchEngine::new(500, 10_000, U256::from(1u64));
        let pool_states = demo_pool_states(address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"));
        let uni_pool = pool_states
            .iter()
            .find(|state| state.dex == base_arb_common::types::DexKind::UniswapV3)
            .unwrap()
            .pool_id
            .address;
        let tick_states = vec![
            TickState {
                pool_id: PoolId {
                    chain_id: 8453,
                    address: uni_pool,
                },
                tick: -1000,
                liquidity_net: 0,
                liquidity_gross: U256::from(1u64),
                block_number: 1,
                updated_at: Utc::now(),
            },
            TickState {
                pool_id: PoolId {
                    chain_id: 8453,
                    address: uni_pool,
                },
                tick: 1000,
                liquidity_net: 0,
                liquidity_gross: U256::from(1u64),
                block_number: 1,
                updated_at: Utc::now(),
            },
        ];
        let candidates = engine.search(&pool_states, &tick_states).await.unwrap();

        assert!(!candidates.is_empty());
        assert!(candidates.iter().all(|c| !c.path.steps.is_empty()));
    }

    #[test]
    fn supported_pool_filter_requires_decimals_for_aerodrome_classic_stable_pools() {
        let usdc = address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913");
        let weth = address!("4200000000000000000000000000000000000006");
        let mut pool_states =
            demo_pool_states(address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"));
        let classic = pool_states
            .iter()
            .position(|state| state.dex == base_arb_common::types::DexKind::Aerodrome)
            .unwrap();
        let config = TokenPairSearchConfig {
            chain_id: 8453,
            token0: usdc,
            token1: weth,
            symbol: "USDC/WETH".into(),
            token0_search_amounts: vec![U256::from(1u64)],
            token1_search_amounts: Vec::new(),
            token0_min_profit: U256::from(1u64),
            token1_min_profit: U256::ZERO,
        };

        pool_states[classic].stable = Some(false);
        assert!(is_supported_config_pool(&pool_states[classic], &config));

        pool_states[classic].stable = Some(true);
        pool_states[classic].token0_decimals = None;
        assert!(!is_supported_config_pool(&pool_states[classic], &config));

        pool_states[classic].token0_decimals = Some(6);
        pool_states[classic].token1_decimals = Some(18);
        assert!(is_supported_config_pool(&pool_states[classic], &config));
    }
}
