use alloy_primitives::{Address, U256};
use std::collections::{HashMap, HashSet};

use base_arb_common::config::Settings;
use base_arb_common::types::{
    ArbPath, Candidate, DexKind, PoolState, PoolVariant, QuoteDiagnostics, QuoteResult,
    QuoteStepDiagnostics, SwapStep, TickState, TokenPairSearchConfig,
};
use base_arb_dex::aerodrome::{AerodromeStableQuoter, AerodromeVolatileQuoter};
use base_arb_dex::quoter::DexQuoter;
use base_arb_dex::uniswap_v3::{quote_exact_in_with_ticks_diagnostics, spot_quote_exact_in};
use tracing::debug;

use crate::opportunity::build_candidate;

#[cfg(test)]
const MAX_FOUR_POOL_CYCLE_PATHS_PER_ANCHOR: usize = 2_000;
const MAX_DYNAMIC_MULTIHOP_PATHS_PER_SCAN: usize = 5_000;
const MAX_DYNAMIC_MULTIHOP_CANDIDATES_PER_SCAN: usize = 20_000;
const MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN: usize = 16;
const MAX_DYNAMIC_SEGMENT_PATHS: usize = 256;

pub struct SearchEngine {
    pub amount_sizes: Vec<U256>,
    pub paths: Vec<ArbPath>,
    pub pair_configs: Vec<TokenPairSearchConfig>,
    pub min_expected_profit: U256,
    pub max_price_impact_bps: u64,
    pub whitelist_paths: Vec<String>,
    pub candidate_ttl_ms: i64,
    pub v3_quote_safety_bps: u64,
    pub quote_max_state_block_lag: u64,
    pub multihop_enabled: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SearchStats {
    pub total_paths: usize,
    pub paths: usize,
    pub quote_attempts: u64,
    pub quote_successes: u64,
    pub quote_skipped: u64,
    pub quote_skipped_missing_state: u64,
    pub quote_skipped_missing_ticks: u64,
    pub quote_skipped_tick_range_exhausted: u64,
    pub quote_skipped_state_block_gap: u64,
    pub quote_skipped_error: u64,
    pub price_impact_rejected: u64,
    pub min_profit_rejected: u64,
    pub candidates_emitted: u64,
    pub dynamic_multihop_paths: u64,
    pub best_profit_before_impact: U256,
    pub best_profit_rejected_by_impact: U256,
    pub best_profit: U256,
}

impl SearchStats {
    pub fn merge(&mut self, other: &SearchStats) {
        self.total_paths += other.total_paths;
        self.paths += other.paths;
        self.quote_attempts += other.quote_attempts;
        self.quote_successes += other.quote_successes;
        self.quote_skipped += other.quote_skipped;
        self.quote_skipped_missing_state += other.quote_skipped_missing_state;
        self.quote_skipped_missing_ticks += other.quote_skipped_missing_ticks;
        self.quote_skipped_tick_range_exhausted += other.quote_skipped_tick_range_exhausted;
        self.quote_skipped_state_block_gap += other.quote_skipped_state_block_gap;
        self.quote_skipped_error += other.quote_skipped_error;
        self.price_impact_rejected += other.price_impact_rejected;
        self.min_profit_rejected += other.min_profit_rejected;
        self.candidates_emitted += other.candidates_emitted;
        self.dynamic_multihop_paths += other.dynamic_multihop_paths;
        self.best_profit_before_impact = self
            .best_profit_before_impact
            .max(other.best_profit_before_impact);
        self.best_profit_rejected_by_impact = self
            .best_profit_rejected_by_impact
            .max(other.best_profit_rejected_by_impact);
        self.best_profit = self.best_profit.max(other.best_profit);
    }

    fn record_quote_skip(&mut self, reason: QuoteSkipReason) {
        self.quote_skipped += 1;
        match reason {
            QuoteSkipReason::MissingState => self.quote_skipped_missing_state += 1,
            QuoteSkipReason::MissingTicks => self.quote_skipped_missing_ticks += 1,
            QuoteSkipReason::TickRangeExhausted => {
                self.quote_skipped_tick_range_exhausted += 1;
            }
            QuoteSkipReason::StateBlockGap => self.quote_skipped_state_block_gap += 1,
            QuoteSkipReason::QuoteError => self.quote_skipped_error += 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QuoteSkipReason {
    MissingState,
    MissingTicks,
    TickRangeExhausted,
    StateBlockGap,
    QuoteError,
}

#[derive(Debug)]
pub struct QuoteSkip {
    reason: QuoteSkipReason,
    message: String,
}

impl QuoteSkip {
    fn new(reason: QuoteSkipReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    fn quote_error(message: impl Into<String>) -> Self {
        Self::new(QuoteSkipReason::QuoteError, message)
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
            quote_max_state_block_lag: 1,
            multihop_enabled: false,
        }
    }

    #[cfg(test)]
    pub async fn search(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
    ) -> anyhow::Result<Vec<Candidate>> {
        Ok(self.search_with_stats(pool_states, tick_states).await?.0)
    }

    #[cfg(test)]
    pub async fn search_with_stats(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
    ) -> anyhow::Result<(Vec<Candidate>, SearchStats)> {
        self.search_with_stats_for_changed_pools(pool_states, tick_states, None)
            .await
    }

    #[cfg(test)]
    pub async fn search_with_stats_for_changed_pools(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
        changed_pools: Option<&HashSet<Address>>,
    ) -> anyhow::Result<(Vec<Candidate>, SearchStats)> {
        let mut paths = self.paths_for_pool_states(pool_states);
        let total_paths = paths.len();
        if let Some(changed_pools) = changed_pools {
            paths.retain(|search_path| {
                search_path
                    .path
                    .steps
                    .iter()
                    .any(|step| changed_pools.contains(&step.pool))
            });
        }
        let mut stats = SearchStats {
            total_paths,
            paths: paths.len(),
            ..SearchStats::default()
        };
        let mut out = Vec::new();
        let quote_context = QuoteContext::new(pool_states, tick_states);

        for search_path in &paths {
            for amount_in in &search_path.amount_sizes {
                stats.quote_attempts += 1;
                let quote = match quote_path(
                    &quote_context,
                    &search_path.path,
                    *amount_in,
                    self.v3_quote_safety_bps,
                    self.quote_max_state_block_lag,
                )
                .await
                {
                    Ok(Some(quote)) => {
                        stats.quote_successes += 1;
                        quote
                    }
                    Ok(None) => {
                        stats.record_quote_skip(QuoteSkipReason::QuoteError);
                        continue;
                    }
                    Err(err) => {
                        stats.record_quote_skip(err.reason);
                        debug!(
                            path = %search_path.path.name,
                            amount_in = %amount_in,
                            reason = ?err.reason,
                            error = %err.message,
                            "quote skipped"
                        );
                        continue;
                    }
                };
                let (expected_amount_out, price_impact_bps, diagnostics) = quote;
                {
                    let expected_profit = expected_amount_out.saturating_sub(*amount_in);
                    stats.best_profit_before_impact =
                        stats.best_profit_before_impact.max(expected_profit);
                    if price_impact_bps > self.max_price_impact_bps {
                        stats.price_impact_rejected += 1;
                        stats.best_profit_rejected_by_impact =
                            stats.best_profit_rejected_by_impact.max(expected_profit);
                        continue;
                    }
                    stats.best_profit = stats.best_profit.max(expected_profit);
                    let required_profit = if search_path.min_profit.is_zero() {
                        self.min_expected_profit
                    } else {
                        search_path.min_profit
                    };
                    if expected_profit < required_profit {
                        stats.min_profit_rejected += 1;
                        continue;
                    }
                    let block_number = candidate_block_number_from_diagnostics(&diagnostics);
                    let candidate = build_candidate(
                        block_number,
                        search_path.strategy.clone(),
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

    pub(crate) fn search_paths_for_path_index(
        &self,
        path_index: &PathIndex,
        changed_pools: &HashSet<Address>,
        dynamic_paths: &[SearchPath],
    ) -> Vec<SearchPath> {
        let path_indices = path_index.path_indices_for_changed_pools(changed_pools);
        path_indices
            .into_iter()
            .map(|index| path_index.paths[index].clone())
            .chain(dynamic_paths.iter().cloned())
            .collect()
    }

    pub(crate) async fn search_with_stats_for_paths(
        &self,
        pool_states: &[PoolState],
        tick_states: &[TickState],
        paths: &[SearchPath],
    ) -> anyhow::Result<(Vec<Candidate>, SearchStats)> {
        let mut stats = SearchStats {
            paths: paths.len(),
            ..SearchStats::default()
        };
        let mut out = Vec::new();
        let quote_context = QuoteContext::new(pool_states, tick_states);

        for search_path in paths {
            self.quote_search_path(search_path, &quote_context, &mut stats, &mut out)
                .await;
        }

        Ok((out, stats))
    }

    async fn quote_search_path(
        &self,
        search_path: &SearchPath,
        quote_context: &QuoteContext<'_>,
        stats: &mut SearchStats,
        out: &mut Vec<Candidate>,
    ) {
        for amount_in in &search_path.amount_sizes {
            stats.quote_attempts += 1;
            let quote = match quote_path(
                quote_context,
                &search_path.path,
                *amount_in,
                self.v3_quote_safety_bps,
                self.quote_max_state_block_lag,
            )
            .await
            {
                Ok(Some(quote)) => {
                    stats.quote_successes += 1;
                    quote
                }
                Ok(None) => {
                    stats.record_quote_skip(QuoteSkipReason::QuoteError);
                    continue;
                }
                Err(err) => {
                    stats.record_quote_skip(err.reason);
                    debug!(
                        path = %search_path.path.name,
                        amount_in = %amount_in,
                        reason = ?err.reason,
                        error = %err.message,
                        "quote skipped"
                    );
                    continue;
                }
            };
            let (expected_amount_out, price_impact_bps, diagnostics) = quote;
            let expected_profit = expected_amount_out.saturating_sub(*amount_in);
            stats.best_profit_before_impact = stats.best_profit_before_impact.max(expected_profit);
            if price_impact_bps > self.max_price_impact_bps {
                stats.price_impact_rejected += 1;
                stats.best_profit_rejected_by_impact =
                    stats.best_profit_rejected_by_impact.max(expected_profit);
                continue;
            }
            stats.best_profit = stats.best_profit.max(expected_profit);
            let required_profit = if search_path.min_profit.is_zero() {
                self.min_expected_profit
            } else {
                search_path.min_profit
            };
            if expected_profit < required_profit {
                stats.min_profit_rejected += 1;
                continue;
            }
            let block_number = candidate_block_number_from_diagnostics(&diagnostics);
            let candidate = build_candidate(
                block_number,
                search_path.strategy.clone(),
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

    pub(crate) fn build_path_index(&self, pool_states: &[PoolState]) -> PathIndex {
        PathIndex::new(self.two_hop_paths_for_pool_states(pool_states))
    }

    pub(crate) fn path_pool_addresses_for_search_paths(
        &self,
        paths: &[SearchPath],
    ) -> HashSet<Address> {
        paths
            .iter()
            .flat_map(|path| path.path.steps.iter().map(|step| step.pool))
            .collect()
    }

    pub(crate) fn build_graph_snapshot(&self, pool_states: &[PoolState]) -> GraphSnapshot {
        GraphSnapshot::new(pool_states)
    }

    pub(crate) fn dynamic_multihop_paths_for_changed_pools(
        &self,
        graph: &GraphSnapshot,
        changed_pools: &HashSet<Address>,
    ) -> Vec<SearchPath> {
        if !self.multihop_enabled || changed_pools.is_empty() {
            return Vec::new();
        }
        let anchors = anchor_search_configs(&self.pair_configs);
        if anchors.is_empty() {
            return Vec::new();
        }

        let mut candidates = Vec::new();
        let mut seen = HashSet::new();
        for anchor in anchors {
            for changed_pool in changed_pools {
                let Some(changed_edges) = graph.pool_edges(*changed_pool) else {
                    continue;
                };
                for changed_edge in changed_edges {
                    add_dynamic_cycles_for_changed_edge(
                        &mut candidates,
                        &mut seen,
                        graph,
                        &anchor,
                        changed_edge,
                        3,
                    );
                    add_dynamic_cycles_for_changed_edge(
                        &mut candidates,
                        &mut seen,
                        graph,
                        &anchor,
                        changed_edge,
                        4,
                    );
                    if candidates.len() >= MAX_DYNAMIC_MULTIHOP_CANDIDATES_PER_SCAN {
                        return select_dynamic_paths(candidates);
                    }
                }
            }
        }
        select_dynamic_paths(candidates)
    }

    #[cfg(test)]
    fn paths_for_pool_states(&self, pool_states: &[PoolState]) -> Vec<SearchPath> {
        let mut paths = self.two_hop_paths_for_pool_states(pool_states);
        if self.multihop_enabled {
            self.add_three_pool_cycle_paths(pool_states, &mut paths);
            self.add_four_pool_cycle_paths(pool_states, &mut paths);
        }

        paths
    }

    fn two_hop_paths_for_pool_states(&self, pool_states: &[PoolState]) -> Vec<SearchPath> {
        if !self.paths.is_empty() {
            return self
                .paths
                .iter()
                .cloned()
                .map(|path| SearchPath {
                    path,
                    amount_sizes: self.amount_sizes.clone(),
                    min_profit: self.min_expected_profit,
                    strategy: "static-path".into(),
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

    #[cfg(test)]
    fn add_three_pool_cycle_paths(&self, pool_states: &[PoolState], paths: &mut Vec<SearchPath>) {
        let anchors = anchor_search_configs(&self.pair_configs);
        if anchors.is_empty() {
            return;
        }

        let edges_by_token = pool_edges_by_token(pool_states);
        let mut seen = HashSet::new();
        for anchor in anchors {
            let Some(first_edges) = edges_by_token.get(&anchor.token) else {
                continue;
            };
            for first in first_edges {
                let Some(second_edges) = edges_by_token.get(&first.token_out) else {
                    continue;
                };
                for second in second_edges {
                    if second.state.pool_id.address == first.state.pool_id.address {
                        continue;
                    }
                    if second.token_out == anchor.token {
                        continue;
                    }
                    let Some(third_edges) = edges_by_token.get(&second.token_out) else {
                        continue;
                    };
                    for third in third_edges {
                        if third.token_out != anchor.token {
                            continue;
                        }
                        if third.state.pool_id.address == first.state.pool_id.address
                            || third.state.pool_id.address == second.state.pool_id.address
                        {
                            continue;
                        }
                        let fingerprint = format!(
                            "{:#x}|{:#x}|{:#x}|{:#x}",
                            anchor.token,
                            first.state.pool_id.address,
                            second.state.pool_id.address,
                            third.state.pool_id.address
                        );
                        if !seen.insert(fingerprint) {
                            continue;
                        }
                        paths.push(build_three_pool_search_path(
                            anchor.clone(),
                            first,
                            second,
                            third,
                        ));
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn add_four_pool_cycle_paths(&self, pool_states: &[PoolState], paths: &mut Vec<SearchPath>) {
        let anchors = anchor_search_configs(&self.pair_configs);
        if anchors.is_empty() {
            return;
        }

        let edges_by_token = pool_edges_by_token(pool_states);
        let mut seen = HashSet::new();
        for anchor in anchors {
            let mut anchor_paths = 0usize;
            let Some(first_edges) = edges_by_token.get(&anchor.token) else {
                continue;
            };
            'anchor: for first in first_edges {
                if first.token_out == anchor.token {
                    continue;
                }
                let Some(second_edges) = edges_by_token.get(&first.token_out) else {
                    continue;
                };
                for second in second_edges {
                    if second.state.pool_id.address == first.state.pool_id.address
                        || second.token_out == anchor.token
                        || second.token_out == first.token_out
                    {
                        continue;
                    }
                    let Some(third_edges) = edges_by_token.get(&second.token_out) else {
                        continue;
                    };
                    for third in third_edges {
                        if third.state.pool_id.address == first.state.pool_id.address
                            || third.state.pool_id.address == second.state.pool_id.address
                            || third.token_out == anchor.token
                            || third.token_out == first.token_out
                            || third.token_out == second.token_out
                        {
                            continue;
                        }
                        let Some(fourth_edges) = edges_by_token.get(&third.token_out) else {
                            continue;
                        };
                        for fourth in fourth_edges {
                            if fourth.token_out != anchor.token {
                                continue;
                            }
                            if fourth.state.pool_id.address == first.state.pool_id.address
                                || fourth.state.pool_id.address == second.state.pool_id.address
                                || fourth.state.pool_id.address == third.state.pool_id.address
                            {
                                continue;
                            }
                            let fingerprint = format!(
                                "{:#x}|{:#x}|{:#x}|{:#x}|{:#x}",
                                anchor.token,
                                first.state.pool_id.address,
                                second.state.pool_id.address,
                                third.state.pool_id.address,
                                fourth.state.pool_id.address
                            );
                            if !seen.insert(fingerprint) {
                                continue;
                            }
                            paths.push(build_four_pool_search_path(
                                anchor.clone(),
                                first,
                                second,
                                third,
                                fourth,
                            ));
                            anchor_paths += 1;
                            if anchor_paths >= MAX_FOUR_POOL_CYCLE_PATHS_PER_ANCHOR {
                                break 'anchor;
                            }
                        }
                    }
                }
            }
        }
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
        quote_max_state_block_lag: settings.quote_max_state_block_lag,
        multihop_enabled: settings.searcher_multihop_enabled,
    })
}

struct QuoteContext<'a> {
    pool_states: HashMap<Address, &'a PoolState>,
    tick_states: HashMap<Address, Vec<TickState>>,
}

impl<'a> QuoteContext<'a> {
    fn new(pool_states: &'a [PoolState], tick_states: &[TickState]) -> Self {
        let pool_states = pool_states
            .iter()
            .map(|state| (state.pool_id.address, state))
            .collect::<HashMap<_, _>>();
        let mut ticks_by_pool: HashMap<Address, Vec<TickState>> = HashMap::new();
        for tick in tick_states {
            ticks_by_pool
                .entry(tick.pool_id.address)
                .or_default()
                .push(tick.clone());
        }
        Self {
            pool_states,
            tick_states: ticks_by_pool,
        }
    }

    fn pool_state(&self, pool: Address) -> Option<&'a PoolState> {
        self.pool_states.get(&pool).copied()
    }

    fn pool_ticks(&self, pool: Address) -> Option<&[TickState]> {
        self.tick_states.get(&pool).map(Vec::as_slice)
    }
}

#[derive(Clone)]
pub(crate) struct SearchPath {
    path: ArbPath,
    amount_sizes: Vec<U256>,
    min_profit: U256,
    strategy: String,
}

impl SearchPath {
    pub(crate) fn all_pools_in(&self, pools: &HashSet<Address>) -> bool {
        self.path
            .steps
            .iter()
            .all(|step| pools.contains(&step.pool))
    }
}

#[derive(Clone)]
pub(crate) struct PathIndex {
    paths: Vec<SearchPath>,
    pool_to_paths: HashMap<Address, Vec<usize>>,
}

impl PathIndex {
    fn new(paths: Vec<SearchPath>) -> Self {
        let mut pool_to_paths: HashMap<Address, Vec<usize>> = HashMap::new();
        for (index, path) in paths.iter().enumerate() {
            for step in &path.path.steps {
                pool_to_paths.entry(step.pool).or_default().push(index);
            }
        }
        Self {
            paths,
            pool_to_paths,
        }
    }

    pub(crate) fn total_paths(&self) -> usize {
        self.paths.len()
    }

    fn path_indices_for_changed_pools(&self, changed_pools: &HashSet<Address>) -> Vec<usize> {
        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        for pool in changed_pools {
            let Some(pool_paths) = self.pool_to_paths.get(pool) else {
                continue;
            };
            for index in pool_paths {
                if seen.insert(*index) {
                    indices.push(*index);
                }
            }
        }
        indices.sort_unstable();
        indices
    }
}

#[derive(Clone)]
struct AnchorSearchConfig {
    token: Address,
    amount_sizes: Vec<U256>,
    min_profit: U256,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct PoolEdge<'a> {
    state: &'a PoolState,
    token_in: Address,
    token_out: Address,
}

#[derive(Clone)]
struct OwnedPoolEdge {
    state: PoolState,
    token_in: Address,
    token_out: Address,
}

impl OwnedPoolEdge {
    fn pool(&self) -> Address {
        self.state.pool_id.address
    }
}

struct ScoredDynamicPath {
    path: SearchPath,
    score_bps: U256,
    estimated_profit: U256,
}

#[derive(Clone)]
pub(crate) struct GraphSnapshot {
    edges_by_token: HashMap<Address, Vec<OwnedPoolEdge>>,
    pool_to_edges: HashMap<Address, Vec<OwnedPoolEdge>>,
}

impl GraphSnapshot {
    fn new(pool_states: &[PoolState]) -> Self {
        let mut edges_by_token: HashMap<Address, Vec<OwnedPoolEdge>> = HashMap::new();
        let mut pool_to_edges: HashMap<Address, Vec<OwnedPoolEdge>> = HashMap::new();
        for state in pool_states.iter().filter(|state| is_supported_pool(state)) {
            let edges = [
                OwnedPoolEdge {
                    state: state.clone(),
                    token_in: state.token0,
                    token_out: state.token1,
                },
                OwnedPoolEdge {
                    state: state.clone(),
                    token_in: state.token1,
                    token_out: state.token0,
                },
            ];
            for edge in edges {
                edges_by_token
                    .entry(edge.token_in)
                    .or_default()
                    .push(edge.clone());
                pool_to_edges.entry(edge.pool()).or_default().push(edge);
            }
        }
        for edges in edges_by_token.values_mut() {
            edges.sort_by_key(|edge| std::cmp::Reverse(pool_depth_score(&edge.state)));
            edges.truncate(MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN);
            edges.sort_by_key(|edge| (edge.token_out, edge.pool()));
        }
        Self {
            edges_by_token,
            pool_to_edges,
        }
    }

    fn edges_from_token(&self, token: Address) -> Option<&[OwnedPoolEdge]> {
        self.edges_by_token.get(&token).map(Vec::as_slice)
    }

    fn pool_edges(&self, pool: Address) -> Option<&[OwnedPoolEdge]> {
        self.pool_to_edges.get(&pool).map(Vec::as_slice)
    }
}

#[cfg(test)]
fn pool_edges_by_token(pool_states: &[PoolState]) -> HashMap<Address, Vec<PoolEdge<'_>>> {
    let mut edges_by_token: HashMap<Address, Vec<PoolEdge<'_>>> = HashMap::new();
    for state in pool_states.iter().filter(|state| is_supported_pool(state)) {
        edges_by_token
            .entry(state.token0)
            .or_default()
            .push(PoolEdge {
                state,
                token_in: state.token0,
                token_out: state.token1,
            });
        edges_by_token
            .entry(state.token1)
            .or_default()
            .push(PoolEdge {
                state,
                token_in: state.token1,
                token_out: state.token0,
            });
    }
    for edges in edges_by_token.values_mut() {
        edges.sort_by_key(|edge| (edge.token_out, edge.state.pool_id.address));
    }
    edges_by_token
}

fn pool_depth_score(state: &PoolState) -> U256 {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            state.reserve0.unwrap_or_default() + state.reserve1.unwrap_or_default()
        }
        PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
            state.liquidity.unwrap_or_default()
        }
    }
}

fn add_dynamic_cycles_for_changed_edge(
    paths: &mut Vec<ScoredDynamicPath>,
    seen: &mut HashSet<String>,
    graph: &GraphSnapshot,
    anchor: &AnchorSearchConfig,
    changed_edge: &OwnedPoolEdge,
    cycle_len: usize,
) {
    for changed_position in 0..cycle_len {
        let prefix_len = changed_position;
        let suffix_len = cycle_len - changed_position - 1;
        let prefixes = edge_paths_between(graph, anchor.token, changed_edge.token_in, prefix_len);
        if prefixes.is_empty() {
            continue;
        }
        let suffixes = edge_paths_between(graph, changed_edge.token_out, anchor.token, suffix_len);
        if suffixes.is_empty() {
            continue;
        }

        for prefix in &prefixes {
            for suffix in &suffixes {
                let mut cycle = Vec::with_capacity(cycle_len);
                cycle.extend(prefix.iter().cloned());
                cycle.push(changed_edge.clone());
                cycle.extend(suffix.iter().cloned());
                if !is_valid_dynamic_cycle(anchor.token, &cycle) {
                    continue;
                }
                let fingerprint = cycle_fingerprint(anchor.token, &cycle);
                if !seen.insert(fingerprint) {
                    continue;
                }
                let Some(scored_path) = score_dynamic_multihop_path(anchor, &cycle) else {
                    continue;
                };
                paths.push(scored_path);
                if paths.len() >= MAX_DYNAMIC_MULTIHOP_CANDIDATES_PER_SCAN {
                    return;
                }
            }
        }
    }
}

fn select_dynamic_paths(mut paths: Vec<ScoredDynamicPath>) -> Vec<SearchPath> {
    paths.sort_by(|left, right| {
        right
            .score_bps
            .cmp(&left.score_bps)
            .then_with(|| right.estimated_profit.cmp(&left.estimated_profit))
            .then_with(|| left.path.path.name.cmp(&right.path.path.name))
    });
    paths
        .into_iter()
        .take(MAX_DYNAMIC_MULTIHOP_PATHS_PER_SCAN)
        .map(|path| path.path)
        .collect()
}

fn score_dynamic_multihop_path(
    anchor: &AnchorSearchConfig,
    edges: &[OwnedPoolEdge],
) -> Option<ScoredDynamicPath> {
    let mut best_score_bps = U256::ZERO;
    let mut best_profit = U256::ZERO;
    for amount_in in anchor.amount_sizes.iter().copied() {
        if amount_in.is_zero() {
            continue;
        }
        let mut amount = amount_in;
        for edge in edges {
            amount = rough_quote_edge_upper_bound(edge, amount)?;
            if amount.is_zero() {
                return None;
            }
        }

        let estimated_profit = amount.saturating_sub(amount_in);
        if estimated_profit < anchor.min_profit {
            continue;
        }
        let score_bps = amount
            .checked_mul(U256::from(10_000u64))
            .and_then(|value| value.checked_div(amount_in))?;
        if score_bps > best_score_bps
            || (score_bps == best_score_bps && estimated_profit > best_profit)
        {
            best_score_bps = score_bps;
            best_profit = estimated_profit;
        }
    }
    if best_score_bps.is_zero() {
        return None;
    }
    Some(ScoredDynamicPath {
        path: build_dynamic_multihop_search_path(anchor, edges),
        score_bps: best_score_bps,
        estimated_profit: best_profit,
    })
}

fn rough_quote_edge_upper_bound(edge: &OwnedPoolEdge, amount_in: U256) -> Option<U256> {
    match edge.state.variant {
        PoolVariant::AerodromeVolatile => {
            let reserve0 = edge.state.reserve0?;
            let reserve1 = edge.state.reserve1?;
            let (reserve_in, reserve_out) = if edge.token_in == edge.state.token0 {
                (reserve0, reserve1)
            } else if edge.token_in == edge.state.token1 {
                (reserve1, reserve0)
            } else {
                return None;
            };
            if edge.state.stable.unwrap_or(false) {
                let decimals0 = edge.state.token0_decimals?;
                let decimals1 = edge.state.token1_decimals?;
                let (decimals_in, decimals_out) = if edge.token_in == edge.state.token0 {
                    (decimals0, decimals1)
                } else {
                    (decimals1, decimals0)
                };
                AerodromeStableQuoter::quote_amount_out(
                    reserve_in,
                    reserve_out,
                    amount_in,
                    edge.state.fee_bps,
                    decimals_in,
                    decimals_out,
                )
                .ok()
            } else {
                AerodromeVolatileQuoter::quote_amount_out(
                    reserve_in,
                    reserve_out,
                    amount_in,
                    edge.state.fee_bps,
                )
                .ok()
            }
        }
        PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
            spot_quote_exact_in(&edge.state, edge.token_in, amount_in).ok()
        }
    }
}

fn edge_paths_between(
    graph: &GraphSnapshot,
    start: Address,
    end: Address,
    edge_count: usize,
) -> Vec<Vec<OwnedPoolEdge>> {
    if edge_count == 0 {
        return if start == end {
            vec![Vec::new()]
        } else {
            Vec::new()
        };
    }
    let mut out = Vec::new();
    let mut current = Vec::with_capacity(edge_count);
    edge_paths_between_inner(graph, start, end, edge_count, &mut current, &mut out);
    out
}

fn edge_paths_between_inner(
    graph: &GraphSnapshot,
    current_token: Address,
    end: Address,
    remaining_edges: usize,
    current: &mut Vec<OwnedPoolEdge>,
    out: &mut Vec<Vec<OwnedPoolEdge>>,
) {
    if out.len() >= MAX_DYNAMIC_SEGMENT_PATHS {
        return;
    }
    if remaining_edges == 0 {
        if current_token == end {
            out.push(current.clone());
        }
        return;
    }
    let Some(edges) = graph.edges_from_token(current_token) else {
        return;
    };
    for edge in edges {
        if out.len() >= MAX_DYNAMIC_SEGMENT_PATHS {
            return;
        }
        if current
            .iter()
            .any(|existing| existing.pool() == edge.pool())
        {
            continue;
        }
        current.push(edge.clone());
        edge_paths_between_inner(
            graph,
            edge.token_out,
            end,
            remaining_edges - 1,
            current,
            out,
        );
        current.pop();
    }
}

fn is_valid_dynamic_cycle(anchor: Address, edges: &[OwnedPoolEdge]) -> bool {
    if edges.is_empty()
        || edges.first().map(|edge| edge.token_in) != Some(anchor)
        || edges.last().map(|edge| edge.token_out) != Some(anchor)
    {
        return false;
    }

    let mut pools = HashSet::new();
    for edge in edges {
        if !pools.insert(edge.pool()) {
            return false;
        }
    }

    let mut intermediate_tokens = HashSet::new();
    for edge in edges.iter().take(edges.len() - 1) {
        if edge.token_out == anchor || !intermediate_tokens.insert(edge.token_out) {
            return false;
        }
    }
    true
}

fn cycle_fingerprint(anchor: Address, edges: &[OwnedPoolEdge]) -> String {
    let mut fingerprint = format!("{anchor:#x}");
    for edge in edges {
        fingerprint.push('|');
        fingerprint.push_str(&format!(
            "{:#x}:{:#x}->{:#x}",
            edge.pool(),
            edge.token_in,
            edge.token_out
        ));
    }
    fingerprint
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
            fee_pips: None,
            stable: Some(false),
            reserve0: Some(U256::from(200_000_000_000u64)),
            reserve1: Some(U256::from(100_000_000_000_000_000_000u128)),
            sqrt_price_x96: None,
            liquidity: None,
            tick: None,
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
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
            fee_pips: Some(3_000),
            stable: None,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(79_228_162_514_264_337_593_543_950_336u128)),
            liquidity: Some(U256::from(1_000_000_000u64)),
            tick: Some(0),
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
            updated_at: now,
        },
    ]
}

async fn quote_path(
    context: &QuoteContext<'_>,
    path: &ArbPath,
    amount_in: U256,
    v3_quote_safety_bps: u64,
    quote_max_state_block_lag: u64,
) -> std::result::Result<Option<(U256, u64, QuoteDiagnostics)>, QuoteSkip> {
    let aero_stable = AerodromeStableQuoter;
    let aero_volatile = AerodromeVolatileQuoter;
    let mut amount = amount_in;
    let mut max_impact = 0u64;
    let mut diagnostics = QuoteDiagnostics {
        modes: Vec::new(),
        ticks_used: 0,
        crossed_ticks: 0,
        tick_range_exhausted: false,
        v3_pools_without_ticks: 0,
        steps: Vec::new(),
    };

    let mut path_max_source_block = 0u64;
    let mut path_min_valid_through_block = u64::MAX;
    for step in &path.steps {
        let pool_state = context.pool_state(step.pool).ok_or_else(|| {
            QuoteSkip::new(
                QuoteSkipReason::MissingState,
                format!("pool state missing for {:#x}", step.pool),
            )
        })?;
        path_max_source_block = path_max_source_block.max(pool_state.block_number);
        path_min_valid_through_block =
            path_min_valid_through_block.min(pool_state.effective_valid_through_block());
    }
    if path_max_source_block
        > path_min_valid_through_block.saturating_add(quote_max_state_block_lag)
    {
        return Err(QuoteSkip::new(
            QuoteSkipReason::StateBlockGap,
            format!(
                "path state block gap max_source={} min_valid_through={} max_lag={}",
                path_max_source_block, path_min_valid_through_block, quote_max_state_block_lag
            ),
        ));
    }

    for (step_index, step) in path.steps.iter().enumerate() {
        let pool_state = match context.pool_state(step.pool) {
            Some(state) => state,
            None => {
                return Err(QuoteSkip::new(
                    QuoteSkipReason::MissingState,
                    format!("pool state missing for {:#x}", step.pool),
                ));
            }
        };

        let amount_before = amount;
        let mut mode = String::new();
        let mut tick_count = 0u32;
        let mut step_ticks_used = 0u32;
        let mut step_crossed_ticks = 0u32;
        let mut step_tick_range_exhausted = false;

        let mut quote = match pool_state.variant {
            PoolVariant::AerodromeVolatile => {
                if pool_state.stable.unwrap_or(false) {
                    aero_stable
                        .quote_exact_in(pool_state, step.token_in, amount)
                        .await
                        .map(|quote| {
                            mode = "classic_stable".into();
                            diagnostics.modes.push(mode.clone());
                            quote
                        })
                        .map_err(|err| QuoteSkip::quote_error(err.to_string()))?
                } else {
                    aero_volatile
                        .quote_exact_in(pool_state, step.token_in, amount)
                        .await
                        .map(|quote| {
                            mode = "classic_volatile".into();
                            diagnostics.modes.push(mode.clone());
                            quote
                        })
                        .map_err(|err| QuoteSkip::quote_error(err.to_string()))?
                }
            }
            PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
                let pool_ticks = context
                    .pool_ticks(pool_state.pool_id.address)
                    .unwrap_or_default();
                tick_count = pool_ticks.len() as u32;
                if pool_ticks.is_empty() {
                    return Err(QuoteSkip::new(
                        QuoteSkipReason::MissingTicks,
                        format!(
                            "initialized tick data missing for {:#x}",
                            pool_state.pool_id.address
                        ),
                    ));
                } else {
                    let (quote, v3_diagnostics) = quote_exact_in_with_ticks_diagnostics(
                        pool_state,
                        &pool_ticks,
                        step.token_in,
                        amount,
                    )
                    .map_err(|err| QuoteSkip::quote_error(err.to_string()))?;
                    mode = "v3_cross_tick".into();
                    diagnostics.modes.push(mode.clone());
                    step_ticks_used = v3_diagnostics.ticks_used;
                    step_crossed_ticks = v3_diagnostics.crossed_ticks;
                    step_tick_range_exhausted = v3_diagnostics.tick_range_exhausted;
                    diagnostics.ticks_used += v3_diagnostics.ticks_used;
                    diagnostics.crossed_ticks += v3_diagnostics.crossed_ticks;
                    diagnostics.tick_range_exhausted |= v3_diagnostics.tick_range_exhausted;
                    quote
                }
            }
        };
        let amount_out_raw = quote.amount_out;
        if is_v3_style_variant(pool_state.variant) && v3_quote_safety_bps > 0 {
            quote.amount_out = apply_quote_haircut(quote.amount_out, v3_quote_safety_bps)
                .map_err(|err| QuoteSkip::quote_error(err.to_string()))?;
        }

        amount = quote.amount_out;
        diagnostics.steps.push(QuoteStepDiagnostics {
            step_no: (step_index + 1) as u32,
            mode,
            pool: pool_state.pool_id.address,
            variant: pool_state.variant,
            source_block: pool_state.block_number,
            valid_through_block: pool_state.effective_valid_through_block(),
            state_updated_at: pool_state.updated_at,
            token_in: step.token_in,
            token_out: step.token_out,
            amount_in: amount_before,
            amount_out_raw,
            amount_out: quote.amount_out,
            fee_bps: pool_state.fee_bps,
            fee_pips: pool_state.fee_pips,
            stable: pool_state.stable,
            tick_spacing: pool_state.tick_spacing,
            sqrt_price_x96: pool_state.sqrt_price_x96,
            liquidity: pool_state.liquidity,
            tick: pool_state.tick,
            reserve0: pool_state.reserve0,
            reserve1: pool_state.reserve1,
            tick_count,
            ticks_used: step_ticks_used,
            crossed_ticks: step_crossed_ticks,
            tick_range_exhausted: step_tick_range_exhausted,
        });
        max_impact = max_impact.max(estimate_price_impact_bps(pool_state, step.token_in, &quote));
    }

    if diagnostics.v3_pools_without_ticks > 0 {
        debug!(
            path = %path.name,
            missing_v3_tick_pools = diagnostics.v3_pools_without_ticks,
            "quote skipped: V3 initialized tick data unavailable"
        );
        return Err(QuoteSkip::new(
            QuoteSkipReason::MissingTicks,
            "V3 initialized tick data unavailable",
        ));
    }
    if diagnostics.tick_range_exhausted {
        debug!(
            path = %path.name,
            ticks_used = diagnostics.ticks_used,
            crossed_ticks = diagnostics.crossed_ticks,
            "quote skipped: V3 quote exhausted known tick range"
        );
        return Err(QuoteSkip::new(
            QuoteSkipReason::TickRangeExhausted,
            "V3 quote exhausted known tick range",
        ));
    }
    Ok(Some((amount, max_impact, diagnostics)))
}

fn candidate_block_number_from_diagnostics(diagnostics: &QuoteDiagnostics) -> u64 {
    diagnostics
        .steps
        .iter()
        .map(|step| step.source_block)
        .max()
        .unwrap_or(0)
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
                swap_step_from_pool(first, token_in, token_mid),
                swap_step_from_pool(second, token_mid, token_in),
            ],
            diagnostics: None,
        },
        amount_sizes,
        min_profit,
        strategy: "pair-two-pool-cycle".into(),
    }
}

#[cfg(test)]
fn build_three_pool_search_path(
    anchor: AnchorSearchConfig,
    first: &PoolEdge<'_>,
    second: &PoolEdge<'_>,
    third: &PoolEdge<'_>,
) -> SearchPath {
    let name = format!(
        "cycle3-{}-{}-{}-{}",
        short_token(anchor.token),
        pool_label(first.state),
        pool_label(second.state),
        pool_label(third.state)
    );
    SearchPath {
        path: ArbPath {
            name,
            steps: vec![
                swap_step_from_pool(first.state, first.token_in, first.token_out),
                swap_step_from_pool(second.state, second.token_in, second.token_out),
                swap_step_from_pool(third.state, third.token_in, third.token_out),
            ],
            diagnostics: None,
        },
        amount_sizes: anchor.amount_sizes,
        min_profit: anchor.min_profit,
        strategy: "token-three-pool-cycle".into(),
    }
}

#[cfg(test)]
fn build_four_pool_search_path(
    anchor: AnchorSearchConfig,
    first: &PoolEdge<'_>,
    second: &PoolEdge<'_>,
    third: &PoolEdge<'_>,
    fourth: &PoolEdge<'_>,
) -> SearchPath {
    let name = format!(
        "cycle4-{}-{}-{}-{}-{}",
        short_token(anchor.token),
        pool_label(first.state),
        pool_label(second.state),
        pool_label(third.state),
        pool_label(fourth.state)
    );
    SearchPath {
        path: ArbPath {
            name,
            steps: vec![
                swap_step_from_pool(first.state, first.token_in, first.token_out),
                swap_step_from_pool(second.state, second.token_in, second.token_out),
                swap_step_from_pool(third.state, third.token_in, third.token_out),
                swap_step_from_pool(fourth.state, fourth.token_in, fourth.token_out),
            ],
            diagnostics: None,
        },
        amount_sizes: anchor.amount_sizes,
        min_profit: anchor.min_profit,
        strategy: "token-four-pool-cycle".into(),
    }
}

fn build_dynamic_multihop_search_path(
    anchor: &AnchorSearchConfig,
    edges: &[OwnedPoolEdge],
) -> SearchPath {
    let pools = edges
        .iter()
        .map(|edge| pool_label(&edge.state))
        .collect::<Vec<_>>()
        .join("-");
    let name = format!(
        "cycle{}-{}-{}",
        edges.len(),
        short_token(anchor.token),
        pools
    );
    SearchPath {
        path: ArbPath {
            name,
            steps: edges
                .iter()
                .map(|edge| swap_step_from_pool(&edge.state, edge.token_in, edge.token_out))
                .collect(),
            diagnostics: None,
        },
        amount_sizes: anchor.amount_sizes.clone(),
        min_profit: anchor.min_profit,
        strategy: format!("token-{}-pool-cycle", edges.len()),
    }
}

fn swap_step_from_pool(state: &PoolState, token_in: Address, token_out: Address) -> SwapStep {
    SwapStep {
        dex: state.dex,
        variant: Some(state.variant),
        factory_address: state.factory_address,
        pool: state.pool_id.address,
        token_in,
        token_out,
        fee_bps: Some(state.fee_bps),
        stable: state.stable,
        tick_spacing: state.tick_spacing,
    }
}

fn anchor_search_configs(configs: &[TokenPairSearchConfig]) -> Vec<AnchorSearchConfig> {
    let mut by_token: HashMap<Address, AnchorSearchConfig> = HashMap::new();
    for config in configs {
        if !config.token0_multihop_search_amounts.is_empty() {
            merge_anchor_config(
                &mut by_token,
                config.token0,
                &config.token0_multihop_search_amounts,
                config.token0_multihop_min_profit,
            );
        }
        if !config.token1_multihop_search_amounts.is_empty() {
            merge_anchor_config(
                &mut by_token,
                config.token1,
                &config.token1_multihop_search_amounts,
                config.token1_multihop_min_profit,
            );
        }
    }
    by_token.into_values().collect()
}

fn merge_anchor_config(
    by_token: &mut HashMap<Address, AnchorSearchConfig>,
    token: Address,
    amounts: &[U256],
    min_profit: U256,
) {
    by_token
        .entry(token)
        .and_modify(|existing| {
            for amount in amounts {
                if !existing.amount_sizes.contains(amount) {
                    existing.amount_sizes.push(*amount);
                }
            }
            existing.min_profit = existing.min_profit.min(min_profit);
        })
        .or_insert_with(|| AnchorSearchConfig {
            token,
            amount_sizes: amounts.to_vec(),
            min_profit,
        });
}

fn is_supported_config_pool(state: &PoolState, config: &TokenPairSearchConfig) -> bool {
    let has_pair = (state.token0 == config.token0 && state.token1 == config.token1)
        || (state.token0 == config.token1 && state.token1 == config.token0);
    if !has_pair {
        return false;
    }
    is_supported_pool(state)
}

fn is_supported_pool(state: &PoolState) -> bool {
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
    use base_arb_common::types::{PoolId, PoolVariant, TickState, TokenPairSearchConfig};
    use chrono::Utc;

    use super::{demo_pool_states, is_supported_config_pool, SearchEngine, SearchPath};

    #[tokio::test]
    async fn search_engine_emits_candidates_for_demo_state() {
        let engine = SearchEngine::new(500, 10_000, U256::ZERO);
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
            token0_multihop_search_amounts: vec![U256::from(1u64)],
            token1_multihop_search_amounts: Vec::new(),
            token0_min_profit: U256::from(1u64),
            token1_min_profit: U256::ZERO,
            token0_multihop_min_profit: U256::from(1u64),
            token1_multihop_min_profit: U256::ZERO,
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

    #[test]
    fn dynamic_paths_include_anchor_three_pool_cycles() {
        let weth = address!("4200000000000000000000000000000000000006");
        let cb_btc = address!("cbb7c0000ab88b473b1f5afd9ef808440eed33bf");
        let sol = address!("311935cd80b76769bf2ecc9d8ab7635b2139cf82");
        let jito_sol = address!("97be14dd8f994a5364573bc035d85309e7cb34de");
        let config = TokenPairSearchConfig {
            chain_id: 8453,
            token0: weth,
            token1: cb_btc,
            symbol: "WETH/cbBTC".into(),
            token0_search_amounts: vec![U256::from(10_000_000_000_000_000u64)],
            token1_search_amounts: Vec::new(),
            token0_multihop_search_amounts: vec![U256::from(10_000_000_000_000_000u64)],
            token1_multihop_search_amounts: Vec::new(),
            token0_min_profit: U256::from(1_000_000_000_000u64),
            token1_min_profit: U256::ZERO,
            token0_multihop_min_profit: U256::from(1_000_000_000_000u64),
            token1_multihop_min_profit: U256::ZERO,
        };
        let engine = SearchEngine {
            amount_sizes: Vec::new(),
            paths: Vec::new(),
            pair_configs: vec![config],
            min_expected_profit: U256::ONE,
            max_price_impact_bps: 10_000,
            whitelist_paths: Vec::new(),
            candidate_ttl_ms: 500,
            v3_quote_safety_bps: 0,
            quote_max_state_block_lag: 0,
            multihop_enabled: true,
        };
        let states = vec![
            test_v3_pool(
                address!("1111111111111111111111111111111111111111"),
                weth,
                sol,
            ),
            test_v3_pool(
                address!("2222222222222222222222222222222222222222"),
                sol,
                jito_sol,
            ),
            test_v3_pool(
                address!("3333333333333333333333333333333333333333"),
                jito_sol,
                weth,
            ),
        ];

        let paths = engine.paths_for_pool_states(&states);

        assert!(paths.iter().any(is_weth_three_pool_cycle));
    }

    #[test]
    fn dynamic_paths_include_anchor_four_pool_cycles() {
        let weth = address!("4200000000000000000000000000000000000006");
        let cb_btc = address!("cbb7c0000ab88b473b1f5afd9ef808440eed33bf");
        let sol = address!("311935cd80b76769bf2ecc9d8ab7635b2139cf82");
        let jito_sol = address!("97be14dd8f994a5364573bc035d85309e7cb34de");
        let usdc = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
        let config = TokenPairSearchConfig {
            chain_id: 8453,
            token0: weth,
            token1: cb_btc,
            symbol: "WETH/cbBTC".into(),
            token0_search_amounts: vec![U256::from(10_000_000_000_000_000u64)],
            token1_search_amounts: Vec::new(),
            token0_multihop_search_amounts: vec![U256::from(10_000_000_000_000_000u64)],
            token1_multihop_search_amounts: Vec::new(),
            token0_min_profit: U256::from(1_000_000_000_000u64),
            token1_min_profit: U256::ZERO,
            token0_multihop_min_profit: U256::from(1_000_000_000_000u64),
            token1_multihop_min_profit: U256::ZERO,
        };
        let engine = SearchEngine {
            amount_sizes: Vec::new(),
            paths: Vec::new(),
            pair_configs: vec![config],
            min_expected_profit: U256::ONE,
            max_price_impact_bps: 10_000,
            whitelist_paths: Vec::new(),
            candidate_ttl_ms: 500,
            v3_quote_safety_bps: 0,
            quote_max_state_block_lag: 0,
            multihop_enabled: true,
        };
        let states = vec![
            test_v3_pool(
                address!("1111111111111111111111111111111111111111"),
                weth,
                sol,
            ),
            test_v3_pool(
                address!("2222222222222222222222222222222222222222"),
                sol,
                jito_sol,
            ),
            test_v3_pool(
                address!("3333333333333333333333333333333333333333"),
                jito_sol,
                usdc,
            ),
            test_v3_pool(
                address!("4444444444444444444444444444444444444444"),
                usdc,
                weth,
            ),
        ];

        let paths = engine.paths_for_pool_states(&states);

        assert!(paths.iter().any(is_weth_four_pool_cycle));
    }

    fn is_weth_three_pool_cycle(path: &SearchPath) -> bool {
        path.strategy == "token-three-pool-cycle"
            && path.path.steps.len() == 3
            && path.path.steps[0].token_in == address!("4200000000000000000000000000000000000006")
            && path.path.steps[2].token_out == address!("4200000000000000000000000000000000000006")
    }

    fn is_weth_four_pool_cycle(path: &SearchPath) -> bool {
        path.strategy == "token-four-pool-cycle"
            && path.path.steps.len() == 4
            && path.path.steps[0].token_in == address!("4200000000000000000000000000000000000006")
            && path.path.steps[3].token_out == address!("4200000000000000000000000000000000000006")
    }

    fn test_v3_pool(
        pool: alloy_primitives::Address,
        token0: alloy_primitives::Address,
        token1: alloy_primitives::Address,
    ) -> base_arb_common::types::PoolState {
        base_arb_common::types::PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: pool,
            },
            dex: base_arb_common::types::DexKind::UniswapV3,
            variant: PoolVariant::UniswapV3,
            factory_address: None,
            token0,
            token1,
            token0_decimals: Some(18),
            token1_decimals: Some(18),
            fee_bps: 1,
            fee_pips: Some(100),
            stable: None,
            reserve0: None,
            reserve1: None,
            sqrt_price_x96: Some(U256::from(79_228_162_514_264_337_593_543_950_336u128)),
            liquidity: Some(U256::from(1_000_000_000u64)),
            tick: Some(0),
            tick_spacing: Some(1),
            block_number: 1,
            valid_through_block: 1,
            updated_at: Utc::now(),
        }
    }
}
