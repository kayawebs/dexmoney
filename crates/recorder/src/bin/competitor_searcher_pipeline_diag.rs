use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    env,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
};

use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::{ChainProvider, TxReceipt};
use base_arb_common::{
    config::Settings,
    constants::{
        AERODROME_CLASSIC_FACTORY, AERODROME_SLIPSTREAM_FACTORIES, PANCAKE_V3_FACTORY,
        UNISWAP_V2_FACTORY, UNISWAP_V3_FACTORY,
    },
    types::{
        ArbPath, DexKind, PoolState, PoolVariant, QuoteDiagnostics, QuoteResult,
        QuoteStepDiagnostics, SwapStep, TickState, TokenPairSearchConfig,
    },
};
use base_arb_dex::{
    aerodrome::{AerodromeStableQuoter, AerodromeVolatileQuoter},
    balancer_v3::quote_weighted_exact_in as quote_balancer_weighted_exact_in,
    quoter::DexQuoter,
    uniswap_v3::{quote_exact_in_with_ticks_diagnostics, spot_quote_exact_in},
};
use base_arb_storage::{redis::RedisStore, PoolStateStore, TickStateStore};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use tracing_subscriber::EnvFilter;

const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const UNI_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const AERODROME_CLASSIC_SWAP_TOPIC: &str =
    "0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b";
const UNISWAP_V4_SWAP_TOPIC: &str =
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
const MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN: usize = 16;
const MAX_DYNAMIC_PRIORITY_EDGES_PER_TOKEN: usize = 32;
const MAX_AMOUNT_TIER_DENOMINATOR: u64 = 1_000_000;
const MAX_AMOUNT_TIER_NUMERATORS: &[u64] = &[
    100, 300, 1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000,
];

#[derive(Debug)]
struct Cli {
    tx_hash: B256,
    output: Option<PathBuf>,
    max_depth: usize,
    max_price_impact_bps: u64,
    max_pool_state_age_ms: i64,
    min_expected_profit: U256,
    shadow_token_amounts: HashMap<Address, Vec<U256>>,
}

#[derive(Debug, Clone)]
struct PoolCoverage {
    pool: Address,
    topic0: String,
    token0: Address,
    token1: Address,
    symbol: String,
    dex: DexKind,
    variant: PoolVariant,
    fee_bps: u32,
    tick_spacing: Option<i32>,
    stable: Option<bool>,
    factory_address: Option<Address>,
    hooks_address: Option<Address>,
    enabled: bool,
    latest_state_block: Option<u64>,
    latest_state_source: Option<String>,
    opportunities_near_block: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct TokenPairSearchConfigRow {
    chain_id: i64,
    token0: String,
    token1: String,
    symbol: String,
    token0_search_amounts: Option<String>,
    token1_search_amounts: Option<String>,
    token0_multihop_search_amounts: Option<String>,
    token1_multihop_search_amounts: Option<String>,
    token0_min_profit: Option<String>,
    token1_min_profit: Option<String>,
    token0_multihop_min_profit: Option<String>,
    token1_multihop_min_profit: Option<String>,
    token0_all_min_profit: Option<String>,
    token1_all_min_profit: Option<String>,
}

#[derive(Debug, Clone)]
struct CycleEdge {
    coverage_index: usize,
    pool: Address,
    token_in: Address,
    token_out: Address,
}

#[derive(Debug, Clone)]
struct AnchorCycle {
    anchor: Address,
    edges: Vec<CycleEdge>,
}

#[derive(Debug, Clone)]
struct AnchorConfig {
    amount_sizes: Vec<U256>,
    min_profit: U256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuoteSkipReason {
    MissingState,
    MissingTicks,
    TickRangeExhausted,
    QuoteError,
}

#[derive(Debug)]
struct QuoteSkip {
    reason: QuoteSkipReason,
    message: String,
    repair_pools: Vec<Address>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoughQuoteFailure {
    MissingReserves,
    TokenMismatch,
    MissingDecimals,
    StableQuoteFailed,
    V2QuoteFailed,
    BalancerQuoteFailed,
    MissingV3State,
    V3SpotQuoteFailed,
    V3SpotOverflow,
    UnsupportedPool,
}

#[derive(Debug, Clone, Copy)]
struct AmountProbe {
    amount: U256,
    source: &'static str,
    production_grid: bool,
}

#[derive(Debug, Clone)]
struct ExactProbeResult {
    amount: U256,
    source: &'static str,
    production_grid: bool,
    stage: String,
    expected_out: Option<U256>,
    expected_profit: Option<U256>,
    required_profit: U256,
    impact_bps: Option<u64>,
    quote_modes: String,
}

#[derive(Debug, Clone)]
struct GraphSnapshot {
    edges_by_token: HashMap<Address, Vec<GraphEdge>>,
    priority_edges_beyond_base_fanout: usize,
    priority_edges_selected: usize,
    priority_edges_dropped: usize,
}

#[derive(Debug, Clone, Copy)]
struct GraphEdge {
    pool: Address,
    token_in: Address,
    token_out: Address,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let cli = parse_args(env::args().skip(1), &settings)?;
    let pg = PgPool::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);

    let mut writer: Box<dyn Write> = match &cli.output {
        Some(path) => {
            Box::new(BufWriter::new(File::create(path).with_context(|| {
                format!("failed to create {}", path.display())
            })?))
        }
        None => Box::new(BufWriter::new(std::io::stdout())),
    };

    run_diag(&mut writer, &cli, &settings, &pg, &redis, &provider).await?;
    writer.flush()?;
    Ok(())
}

async fn run_diag(
    writer: &mut dyn Write,
    cli: &Cli,
    settings: &Settings,
    pg: &PgPool,
    redis: &RedisStore,
    provider: &ChainProvider,
) -> Result<()> {
    let receipt = provider
        .get_transaction_receipt(cli.tx_hash)
        .await?
        .with_context(|| format!("transaction receipt not found: {:#x}", cli.tx_hash))?;
    let tx_block = receipt
        .block_number
        .context("transaction receipt missing block_number")?;
    let pool_logs = dedupe_swap_pools(receipt_swap_logs(&receipt));
    let pair_configs = load_enabled_pair_search_configs(pg).await?;
    let anchor_configs = anchor_search_configs(&pair_configs);
    let anchor_tokens = anchor_configs
        .keys()
        .copied()
        .collect::<BTreeSet<Address>>();

    let mut coverages = Vec::new();
    for (pool, topic0) in &pool_logs {
        coverages.push(load_pool_coverage(pg, settings.chain_id, tx_block, *pool, topic0).await?);
    }
    let priority_pools = coverages
        .iter()
        .map(|coverage| coverage.pool)
        .collect::<HashSet<_>>();
    let cycles = anchor_cycles(&coverages, &anchor_tokens, cli.max_depth);
    let observed_anchor_amounts = observed_pool_to_pool_anchor_amounts(&receipt, &coverages);
    let current_block = provider.get_block_number().await.unwrap_or_default();
    let redis_states = redis.all_pool_states().await?;
    let active_states = redis_states
        .iter()
        .filter(|state| {
            is_pool_state_active(state, Utc::now(), cli.max_pool_state_age_ms, settings)
                || (priority_pools.contains(&state.pool_id.address)
                    && is_pool_state_quote_ready(state, settings))
        })
        .cloned()
        .collect::<Vec<_>>();
    let active_pools = active_states
        .iter()
        .map(|state| state.pool_id.address)
        .collect::<HashSet<_>>();
    let graph = GraphSnapshot::new(&active_states, &priority_pools);

    writeln!(writer, "competitor searcher pipeline diagnostic")?;
    writeln!(writer, "tx_hash: {:#x}", cli.tx_hash)?;
    writeln!(writer, "tx_block: {tx_block}")?;
    writeln!(writer, "current_block: {current_block}")?;
    writeln!(writer, "state_snapshot: current_redis_at_run_time")?;
    writeln!(writer, "success: {}", receipt.success)?;
    writeln!(
        writer,
        "configured_max_price_impact_bps: {}",
        cli.max_price_impact_bps
    )?;
    writeln!(
        writer,
        "configured_min_expected_profit: {}",
        cli.min_expected_profit
    )?;
    writeln!(
        writer,
        "shadow_token_amounts: {}",
        display_shadow_token_amounts(&cli.shadow_token_amounts)
    )?;
    writeln!(
        writer,
        "redis_pool_states: {} active_pool_states: {} priority_pools: {} priority_edges_beyond_base_fanout: {} priority_edges_selected: {} priority_edges_dropped: {}",
        redis_states.len(),
        active_states.len(),
        priority_pools.len(),
        graph.priority_edges_beyond_base_fanout,
        graph.priority_edges_selected,
        graph.priority_edges_dropped
    )?;
    writeln!(writer, "topic_summary: {}", receipt_topic_summary(&receipt))?;
    writeln!(
        writer,
        "recognized_swap_pools: {} recognized_anchor_cycles: {}",
        coverages.len(),
        cycles.len()
    )?;
    writeln!(writer)?;

    writeln!(writer, "== Transfer Flow ==")?;
    for transfer in receipt_transfer_flow(&receipt) {
        writeln!(writer, "{transfer}")?;
    }
    writeln!(writer)?;

    writeln!(writer, "== Pools ==")?;
    for coverage in &coverages {
        let redis_state = redis.get_pool_state(coverage.pool).await?;
        let ticks = redis
            .get_pool_ticks(coverage.pool)
            .await
            .unwrap_or_default();
        writeln!(
            writer,
            "pool={:#x} topic={} symbol={} dex={:?} variant={:?} enabled={} db_state_block={} db_state_source={} redis_state={} redis_block={} redis_updated_at={} active={} redis_ticks={} opportunities_near_block={}",
            coverage.pool,
            topic_family(&coverage.topic0),
            coverage.symbol,
            coverage.dex,
            coverage.variant,
            coverage.enabled,
            display_opt_u64(coverage.latest_state_block),
            coverage.latest_state_source.as_deref().unwrap_or("-"),
            redis_state.is_some(),
            redis_state
                .as_ref()
                .map(|state| state.block_number.to_string())
                .unwrap_or_else(|| "-".into()),
            redis_state
                .as_ref()
                .map(|state| state.updated_at.to_string())
                .unwrap_or_else(|| "-".into()),
            active_pools.contains(&coverage.pool),
            ticks.len(),
            coverage.opportunities_near_block
        )?;
    }
    writeln!(writer)?;

    if cycles.is_empty() {
        writeln!(writer, "== Decision ==")?;
        writeln!(writer, "root_stage: path_generation")?;
        writeln!(
            writer,
            "reason: no recognized anchor cycle can be built from supported swap pools and configured anchor tokens"
        )?;
        return Ok(());
    }

    let mut cycle_summaries = Vec::new();
    for (cycle_idx, cycle) in cycles.iter().enumerate() {
        let summary = diagnose_cycle(
            writer,
            cycle_idx + 1,
            cycle,
            &receipt,
            &coverages,
            &pair_configs,
            &anchor_configs,
            &observed_anchor_amounts,
            redis,
            &graph,
            &active_pools,
            settings,
            cli,
        )
        .await?;
        cycle_summaries.push(summary);
    }

    writeln!(writer, "== Decision ==")?;
    let root = decide_root_stage(&cycle_summaries);
    writeln!(writer, "root_stage: {}", root)?;
    writeln!(
        writer,
        "root_detail: {}",
        cycle_summaries
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(" | ")
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn diagnose_cycle(
    writer: &mut dyn Write,
    cycle_no: usize,
    cycle: &AnchorCycle,
    receipt: &TxReceipt,
    coverages: &[PoolCoverage],
    pair_configs: &[TokenPairSearchConfig],
    anchor_configs: &HashMap<Address, AnchorConfig>,
    observed_anchor_amounts: &BTreeMap<Address, U256>,
    redis: &RedisStore,
    graph: &GraphSnapshot,
    active_pools: &HashSet<Address>,
    _settings: &Settings,
    cli: &Cli,
) -> Result<String> {
    let path = build_path(cycle, coverages);
    let target_pools = cycle
        .edges
        .iter()
        .map(|edge| format!("{:#x}", edge.pool))
        .collect::<Vec<_>>();
    let states = load_cycle_states(redis, cycle).await?;
    let ticks = load_cycle_ticks(redis, cycle).await?;
    let all_states_present = states.values().all(Option::is_some);
    let all_active = cycle
        .edges
        .iter()
        .all(|edge| active_pools.contains(&edge.pool));
    let changed_pool_trigger_inferred = cycle.edges.iter().any(|edge| {
        coverages
            .get(edge.coverage_index)
            .is_some_and(|coverage| coverage.topic0 != ERC20_TRANSFER_TOPIC)
    });
    let path_generated =
        path_generated_status(cycle, pair_configs, anchor_configs, graph, active_pools);
    let amount_config = amount_config_for_cycle(cycle, pair_configs, anchor_configs);
    let observed_anchor_input = observed_cycle_anchor_input(receipt, cycle)
        .or_else(|| observed_anchor_amounts.get(&cycle.anchor).copied());
    let mut probes = amount_config
        .amounts
        .iter()
        .copied()
        .map(|amount| AmountProbe {
            amount,
            source: "configured_grid",
            production_grid: true,
        })
        .collect::<Vec<_>>();
    if let Some(observed_amount) = observed_anchor_input {
        if !observed_amount.is_zero() && !amount_config.amounts.contains(&observed_amount) {
            probes.push(AmountProbe {
                amount: observed_amount,
                source: "observed_competitor_input_shadow",
                production_grid: false,
            });
        }
    }
    if let Some(shadow_amounts) = cli.shadow_token_amounts.get(&cycle.anchor) {
        for shadow_amount in shadow_amounts {
            if shadow_amount.is_zero() {
                continue;
            }
            if probes.iter().any(|probe| probe.amount == *shadow_amount) {
                continue;
            }
            probes.push(AmountProbe {
                amount: *shadow_amount,
                source: "shadow_capital_tier",
                production_grid: false,
            });
        }
    }
    probes.sort_by(|left, right| {
        left.production_grid
            .cmp(&right.production_grid)
            .reverse()
            .then_with(|| left.amount.cmp(&right.amount))
    });

    writeln!(writer, "== Cycle {cycle_no} ==")?;
    writeln!(writer, "path_name: {}", path.name)?;
    writeln!(writer, "anchor: {:#x}", cycle.anchor)?;
    writeln!(writer, "pools: {}", target_pools.join(","))?;
    writeln!(writer, "steps:")?;
    for step in &path.steps {
        writeln!(
            writer,
            "  {}->{}, pool={:#x}, dex={:?}, variant={:?}",
            short_addr(step.token_in),
            short_addr(step.token_out),
            step.pool,
            step.dex,
            step.variant
        )?;
    }
    writeln!(
        writer,
        "path_generated: {}",
        if path_generated.generated {
            "yes"
        } else {
            "no"
        }
    )?;
    writeln!(writer, "path_generation_reason: {}", path_generated.reason)?;
    writeln!(writer, "all_redis_states_present: {all_states_present}")?;
    writeln!(writer, "all_pools_active_now: {all_active}")?;
    writeln!(
        writer,
        "changed_pool_trigger_inferred_from_tx: {changed_pool_trigger_inferred}"
    )?;
    writeln!(
        writer,
        "configured_amounts: {}",
        amount_config
            .amounts
            .iter()
            .map(U256::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )?;
    writeln!(writer, "required_profit: {}", amount_config.min_profit)?;
    writeln!(
        writer,
        "observed_anchor_input_shadow: {}",
        observed_anchor_input
            .map(|amount| amount.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        writer,
        "shadow_configured_amounts_for_anchor: {}",
        cli.shadow_token_amounts
            .get(&cycle.anchor)
            .map(|amounts| amounts
                .iter()
                .map(U256::to_string)
                .collect::<Vec<_>>()
                .join(","))
            .unwrap_or_else(|| "-".into())
    )?;

    let rough = rough_quote_cycle(cycle, &states, &amount_config);
    writeln!(writer, "rough_quote: {}", rough)?;

    let mut exact_results = Vec::new();
    for probe in probes {
        let result =
            exact_quote_probe(&path, &states, &ticks, probe, amount_config.min_profit, cli).await;
        match result {
            Ok(result) => {
                writeln!(
                    writer,
                    "exact_probe amount={} source={} production_grid={} stage={} expected_out={} expected_profit={} required_profit={} impact_bps={} modes={}",
                    result.amount,
                    result.source,
                    result.production_grid,
                    result.stage,
                    result
                        .expected_out
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    result
                        .expected_profit
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    result.required_profit,
                    result
                        .impact_bps
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    result.quote_modes
                )?;
                exact_results.push(result);
            }
            Err(err) => {
                writeln!(
                    writer,
                    "exact_probe amount={} source={} production_grid={} stage=quote_error error={:#}",
                    probe.amount, probe.source, probe.production_grid, err
                )?;
                exact_results.push(ExactProbeResult {
                    amount: probe.amount,
                    source: probe.source,
                    production_grid: probe.production_grid,
                    stage: "quote_error".into(),
                    expected_out: None,
                    expected_profit: None,
                    required_profit: amount_config.min_profit,
                    impact_bps: None,
                    quote_modes: "-".into(),
                });
            }
        }
    }
    writeln!(writer)?;

    Ok(summarize_cycle(
        path_generated.generated,
        &rough,
        &exact_results,
    ))
}

fn path_generated_status(
    cycle: &AnchorCycle,
    pair_configs: &[TokenPairSearchConfig],
    anchor_configs: &HashMap<Address, AnchorConfig>,
    graph: &GraphSnapshot,
    active_pools: &HashSet<Address>,
) -> PathGeneratedStatus {
    if cycle.edges.len() == 2 {
        let inactive = cycle
            .edges
            .iter()
            .filter(|edge| !active_pools.contains(&edge.pool))
            .map(|edge| format!("pool excluded by active-state guard pool={:#x}", edge.pool))
            .collect::<Vec<_>>();
        if !inactive.is_empty() {
            return PathGeneratedStatus::no(inactive.join("; "));
        }
        let first = &cycle.edges[0];
        let pair_config = pair_configs.iter().find(|config| {
            (config.token0 == first.token_in && config.token1 == first.token_out)
                || (config.token1 == first.token_in && config.token0 == first.token_out)
        });
        let Some(config) = pair_config else {
            return PathGeneratedStatus::no("missing two-hop token_pair search config");
        };
        let has_amounts = if cycle.anchor == config.token0 {
            !config.token0_search_amounts.is_empty()
        } else if cycle.anchor == config.token1 {
            !config.token1_search_amounts.is_empty()
        } else {
            false
        };
        if !has_amounts {
            return PathGeneratedStatus::no("two-hop pair config has no search amounts for anchor");
        }
        return PathGeneratedStatus::yes("two-hop pair path exists in static path index");
    }

    if !anchor_configs.contains_key(&cycle.anchor) {
        return PathGeneratedStatus::no("missing multihop anchor search config");
    }
    let mut rejected_edges = Vec::new();
    for edge in &cycle.edges {
        if !active_pools.contains(&edge.pool) {
            rejected_edges.push(format!(
                "pool excluded by active-state guard pool={:#x}",
                edge.pool
            ));
            continue;
        }
        let included = graph
            .edges_by_token
            .get(&edge.token_in)
            .is_some_and(|edges| {
                edges.iter().any(|candidate| {
                    candidate.pool == edge.pool
                        && candidate.token_in == edge.token_in
                        && candidate.token_out == edge.token_out
                })
            });
        if !included {
            rejected_edges.push(format!(
                "edge excluded by dynamic graph fanout token={} pool={:#x}",
                short_addr(edge.token_in),
                edge.pool
            ));
        }
    }
    if !rejected_edges.is_empty() {
        return PathGeneratedStatus::no(rejected_edges.join("; "));
    }
    PathGeneratedStatus::yes("all cycle edges are present in dynamic graph fanout")
}

struct PathGeneratedStatus {
    generated: bool,
    reason: String,
}

impl PathGeneratedStatus {
    fn yes(reason: impl Into<String>) -> Self {
        Self {
            generated: true,
            reason: reason.into(),
        }
    }

    fn no(reason: impl Into<String>) -> Self {
        Self {
            generated: false,
            reason: reason.into(),
        }
    }
}

struct AmountConfig {
    amounts: Vec<U256>,
    min_profit: U256,
}

fn amount_config_for_cycle(
    cycle: &AnchorCycle,
    pair_configs: &[TokenPairSearchConfig],
    anchor_configs: &HashMap<Address, AnchorConfig>,
) -> AmountConfig {
    if cycle.edges.len() == 2 {
        if let Some(config) = pair_configs.iter().find(|config| {
            (config.token0 == cycle.edges[0].token_in && config.token1 == cycle.edges[0].token_out)
                || (config.token1 == cycle.edges[0].token_in
                    && config.token0 == cycle.edges[0].token_out)
        }) {
            if cycle.anchor == config.token0 {
                return AmountConfig {
                    amounts: expand_max_amounts(&config.token0_search_amounts),
                    min_profit: config.token0_min_profit,
                };
            }
            if cycle.anchor == config.token1 {
                return AmountConfig {
                    amounts: expand_max_amounts(&config.token1_search_amounts),
                    min_profit: config.token1_min_profit,
                };
            }
        }
    }
    anchor_configs
        .get(&cycle.anchor)
        .map(|config| AmountConfig {
            amounts: config.amount_sizes.clone(),
            min_profit: config.min_profit,
        })
        .unwrap_or_else(|| AmountConfig {
            amounts: Vec::new(),
            min_profit: U256::ZERO,
        })
}

async fn load_cycle_states(
    redis: &RedisStore,
    cycle: &AnchorCycle,
) -> Result<HashMap<Address, Option<PoolState>>> {
    let mut out = HashMap::new();
    for edge in &cycle.edges {
        out.insert(edge.pool, redis.get_pool_state(edge.pool).await?);
    }
    Ok(out)
}

async fn load_cycle_ticks(
    redis: &RedisStore,
    cycle: &AnchorCycle,
) -> Result<HashMap<Address, Vec<TickState>>> {
    let mut out = HashMap::new();
    for edge in &cycle.edges {
        let ticks = redis.get_pool_ticks(edge.pool).await.unwrap_or_default();
        out.insert(edge.pool, ticks);
    }
    Ok(out)
}

fn rough_quote_cycle(
    cycle: &AnchorCycle,
    states: &HashMap<Address, Option<PoolState>>,
    amount_config: &AmountConfig,
) -> String {
    if amount_config.amounts.is_empty() {
        return "not_generated: no configured amounts".into();
    }
    let mut best_profit = U256::ZERO;
    let mut best_amount = U256::ZERO;
    let mut first_failure = None;
    for amount_in in &amount_config.amounts {
        let mut amount = *amount_in;
        let mut failed = None;
        for edge in &cycle.edges {
            let Some(Some(state)) = states.get(&edge.pool) else {
                failed = Some(format!("missing_state pool={:#x}", edge.pool));
                break;
            };
            match rough_quote_edge(edge, state, amount) {
                Ok(next) if !next.is_zero() => amount = next,
                Ok(_) => {
                    failed = Some(format!("ZeroOutput pool={:#x}", edge.pool));
                    break;
                }
                Err(reason) => {
                    failed = Some(format!("{:?} pool={:#x}", reason, edge.pool));
                    break;
                }
            }
        }
        if let Some(failed) = failed {
            first_failure.get_or_insert(failed);
            continue;
        }
        let profit = amount.saturating_sub(*amount_in);
        if profit > best_profit {
            best_profit = profit;
            best_amount = *amount_in;
        }
    }
    if best_profit >= amount_config.min_profit {
        format!(
            "included_after_rough best_amount={} best_profit={} min={}",
            best_amount, best_profit, amount_config.min_profit
        )
    } else if !best_profit.is_zero() {
        format!(
            "dropped_rough_profit_below_min best_amount={} best_profit={} min={}",
            best_amount, best_profit, amount_config.min_profit
        )
    } else {
        format!(
            "dropped_rough_quote_failed first_failure={}",
            first_failure.unwrap_or_else(|| "all_zero_or_no_amounts".into())
        )
    }
}

async fn exact_quote_probe(
    path: &ArbPath,
    states: &HashMap<Address, Option<PoolState>>,
    ticks: &HashMap<Address, Vec<TickState>>,
    probe: AmountProbe,
    required_profit: U256,
    cli: &Cli,
) -> Result<ExactProbeResult> {
    let (expected_out, impact_bps, diagnostics) = quote_path(path, states, ticks, probe.amount)
        .await
        .map_err(|skip| {
            anyhow::anyhow!(
                "quote_skipped reason={:?} message={} repair_pools={}",
                skip.reason,
                skip.message,
                skip.repair_pools
                    .iter()
                    .map(|pool| format!("{pool:#x}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })?;
    let expected_profit = expected_out.saturating_sub(probe.amount);
    let quote_edge_floor = quote_model_edge_floor(probe.amount, &diagnostics, 10);
    let stage = if expected_profit < required_profit {
        "min_profit_rejected"
    } else if impact_bps > cli.max_price_impact_bps {
        "price_impact_rejected"
    } else if expected_profit < quote_edge_floor {
        "quote_model_edge_rejected"
    } else if probe.production_grid {
        "candidate_publish_eligible"
    } else {
        "shadow_would_publish_if_amount_were_in_grid"
    };
    Ok(ExactProbeResult {
        amount: probe.amount,
        source: probe.source,
        production_grid: probe.production_grid,
        stage: stage.into(),
        expected_out: Some(expected_out),
        expected_profit: Some(expected_profit),
        required_profit,
        impact_bps: Some(impact_bps),
        quote_modes: diagnostics.modes.join("+"),
    })
}

async fn quote_path(
    path: &ArbPath,
    states: &HashMap<Address, Option<PoolState>>,
    ticks: &HashMap<Address, Vec<TickState>>,
    amount_in: U256,
) -> std::result::Result<(U256, u64, QuoteDiagnostics), QuoteSkip> {
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

    for step in &path.steps {
        if !states.get(&step.pool).is_some_and(Option::is_some) {
            return Err(QuoteSkip::new(
                QuoteSkipReason::MissingState,
                format!("pool state missing for {:#x}", step.pool),
            ));
        }
    }

    for (step_index, step) in path.steps.iter().enumerate() {
        let pool_state = states
            .get(&step.pool)
            .and_then(Option::as_ref)
            .expect("state presence checked above");
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
            PoolVariant::AerodromeSlipstream
            | PoolVariant::UniswapV3
            | PoolVariant::PancakeV3
            | PoolVariant::UniswapV4 => {
                let pool_ticks = ticks
                    .get(&pool_state.pool_id.address)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                tick_count = pool_ticks.len() as u32;
                if pool_ticks.is_empty() {
                    return Err(QuoteSkip::with_repair_pool(
                        QuoteSkipReason::MissingTicks,
                        format!(
                            "initialized tick data missing for {:#x}",
                            pool_state.pool_id.address
                        ),
                        pool_state.pool_id.address,
                    ));
                }
                let (quote, v3_diagnostics) = quote_exact_in_with_ticks_diagnostics(
                    pool_state,
                    pool_ticks,
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
            PoolVariant::BalancerV3 => {
                if pool_state.balancer_model.as_deref() != Some("weighted") {
                    return Err(QuoteSkip::quote_error(
                        "Balancer V3 local model missing and runtime router quote disabled",
                    ));
                }
                let amount_out = quote_balancer_weighted_exact_in(
                    pool_state,
                    step.token_in,
                    step.token_out,
                    amount,
                )
                .map_err(QuoteSkip::quote_error)?;
                mode = "balancer_v3_weighted_local".into();
                diagnostics.modes.push(mode.clone());
                QuoteResult {
                    amount_in: amount,
                    amount_out,
                    gas_estimate: None,
                }
            }
        };
        let amount_out_raw = quote.amount_out;
        if is_v3_style_variant(pool_state.variant) {
            quote.amount_out = apply_quote_haircut(quote.amount_out, 10)
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
    if diagnostics.tick_range_exhausted {
        return Err(QuoteSkip::with_repair_pools(
            QuoteSkipReason::TickRangeExhausted,
            "V3 quote exhausted known tick range",
            tick_repair_pools_from_diagnostics(&diagnostics),
        ));
    }
    Ok((amount, max_impact, diagnostics))
}

fn build_path(cycle: &AnchorCycle, coverages: &[PoolCoverage]) -> ArbPath {
    let pools = cycle
        .edges
        .iter()
        .map(|edge| short_hex(format!("{:#x}", edge.pool)))
        .collect::<Vec<_>>()
        .join("-");
    ArbPath {
        name: format!(
            "diag-cycle{}-{}-{pools}",
            cycle.edges.len(),
            short_addr(cycle.anchor)
        ),
        steps: cycle
            .edges
            .iter()
            .map(|edge| {
                let coverage = &coverages[edge.coverage_index];
                SwapStep {
                    dex: coverage.dex,
                    variant: Some(coverage.variant),
                    factory_address: coverage.factory_address,
                    pool: edge.pool,
                    token_in: edge.token_in,
                    token_out: edge.token_out,
                    fee_bps: Some(coverage.fee_bps),
                    pool_key_fee_pips: None,
                    hooks_address: coverage.hooks_address,
                    stable: coverage.stable,
                    tick_spacing: coverage.tick_spacing,
                    adapter_data: None,
                }
            })
            .collect(),
        diagnostics: None,
    }
}

fn summarize_cycle(path_generated: bool, rough: &str, results: &[ExactProbeResult]) -> String {
    if !path_generated {
        return "path_generation_rejected".into();
    }
    if results
        .iter()
        .any(|result| result.stage == "shadow_would_publish_if_amount_were_in_grid")
    {
        return "shadow_amount_would_publish_not_in_live_grid".into();
    }
    if rough.starts_with("dropped_rough") {
        if results
            .iter()
            .any(|result| result.stage == "candidate_publish_eligible")
        {
            return format!("rough_quote_pruned_but_exact_grid_passes: {rough}");
        }
        if results.iter().any(|result| {
            result.stage == "shadow_would_publish_if_amount_were_in_grid"
                || (!result.production_grid && result.stage == "price_impact_rejected")
        }) {
            return format!("rough_quote_or_amount_grid_suspect: {rough}");
        }
        return format!("rough_quote_pruned: {rough}");
    }
    if results
        .iter()
        .any(|result| result.stage == "candidate_publish_eligible")
    {
        return "candidate_publish_eligible_current_state".into();
    }
    let production_results = results
        .iter()
        .filter(|result| result.production_grid)
        .collect::<Vec<_>>();
    if production_results.is_empty() && !results.is_empty() {
        return "shadow_only_no_live_amount_grid".into();
    }
    if results
        .iter()
        .filter(|result| result.production_grid)
        .all(|result| result.stage == "min_profit_rejected")
    {
        return "min_profit_rejected_on_configured_grid".into();
    }
    if results
        .iter()
        .filter(|result| result.production_grid)
        .all(|result| {
            result.stage == "min_profit_rejected" || result.stage == "price_impact_rejected"
        })
    {
        return "configured_grid_rejected_by_min_profit_or_impact".into();
    }
    if results
        .iter()
        .any(|result| result.stage == "quote_error" || result.stage.contains("quote"))
    {
        return "quote_skip_or_quote_error".into();
    }
    "unknown_after_exact_quote".into()
}

fn decide_root_stage(summaries: &[String]) -> &'static str {
    if summaries
        .iter()
        .all(|summary| summary == "path_generation_rejected")
    {
        "path_generation"
    } else if summaries
        .iter()
        .any(|summary| summary.contains("rough_quote"))
    {
        "rough_quote_or_dynamic_path_pruning"
    } else if summaries
        .iter()
        .any(|summary| summary.contains("shadow_amount"))
    {
        "amount_grid_or_capital_config"
    } else if summaries
        .iter()
        .all(|summary| summary.contains("min_profit"))
    {
        "min_profit"
    } else if summaries.iter().all(|summary| summary.contains("impact")) {
        "impact"
    } else if summaries
        .iter()
        .any(|summary| summary.contains("candidate_publish_eligible"))
    {
        "candidate_publish_or_coalesce_window"
    } else {
        "mixed_or_unknown"
    }
}

impl GraphSnapshot {
    fn new(pool_states: &[PoolState], priority_pools: &HashSet<Address>) -> Self {
        let mut edges_by_token: HashMap<Address, Vec<(GraphEdge, U256)>> = HashMap::new();
        for state in pool_states.iter().filter(|state| is_supported_pool(state)) {
            let depth = pool_depth_score(state);
            for (token_in, token_out) in
                [(state.token0, state.token1), (state.token1, state.token0)]
            {
                edges_by_token.entry(token_in).or_default().push((
                    GraphEdge {
                        pool: state.pool_id.address,
                        token_in,
                        token_out,
                    },
                    depth,
                ));
            }
        }
        let mut priority_edges_beyond_base_fanout = 0usize;
        let mut priority_edges_selected = 0usize;
        let mut priority_edges_dropped = 0usize;
        let edges_by_token = edges_by_token
            .into_iter()
            .map(|(token, mut edges)| {
                edges.sort_by(|left, right| {
                    right
                        .1
                        .cmp(&left.1)
                        .then_with(|| left.0.token_out.cmp(&right.0.token_out))
                        .then_with(|| left.0.pool.cmp(&right.0.pool))
                });
                priority_edges_beyond_base_fanout += edges
                    .iter()
                    .skip(MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN)
                    .filter(|(edge, _)| priority_pools.contains(&edge.pool))
                    .count();
                let mut selected = edges
                    .iter()
                    .take(MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN)
                    .map(|(edge, _)| *edge)
                    .collect::<Vec<_>>();
                let mut token_priority_selected = 0usize;
                for (edge, _) in edges
                    .iter()
                    .skip(MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN)
                    .filter(|(edge, _)| priority_pools.contains(&edge.pool))
                {
                    if token_priority_selected < MAX_DYNAMIC_PRIORITY_EDGES_PER_TOKEN {
                        selected.push(*edge);
                        priority_edges_selected += 1;
                        token_priority_selected += 1;
                    } else {
                        priority_edges_dropped += 1;
                    }
                }
                selected.sort_by_key(|edge| (edge.token_out, edge.pool));
                (token, selected)
            })
            .collect();
        Self {
            edges_by_token,
            priority_edges_beyond_base_fanout,
            priority_edges_selected,
            priority_edges_dropped,
        }
    }
}

fn anchor_search_configs(configs: &[TokenPairSearchConfig]) -> HashMap<Address, AnchorConfig> {
    let mut by_token: HashMap<Address, AnchorConfig> = HashMap::new();
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
    by_token
}

fn merge_anchor_config(
    by_token: &mut HashMap<Address, AnchorConfig>,
    token: Address,
    amounts: &[U256],
    min_profit: U256,
) {
    by_token
        .entry(token)
        .and_modify(|existing| {
            for amount in expand_max_amounts(amounts) {
                if !existing.amount_sizes.contains(&amount) {
                    existing.amount_sizes.push(amount);
                }
            }
            existing.amount_sizes.sort();
            existing.min_profit = existing.min_profit.min(min_profit);
        })
        .or_insert_with(|| AnchorConfig {
            amount_sizes: expand_max_amounts(amounts),
            min_profit,
        });
}

fn anchor_cycles(
    coverages: &[PoolCoverage],
    anchor_tokens: &BTreeSet<Address>,
    max_depth: usize,
) -> Vec<AnchorCycle> {
    let edges = coverages
        .iter()
        .enumerate()
        .flat_map(|(idx, coverage)| {
            [
                CycleEdge {
                    coverage_index: idx,
                    pool: coverage.pool,
                    token_in: coverage.token0,
                    token_out: coverage.token1,
                },
                CycleEdge {
                    coverage_index: idx,
                    pool: coverage.pool,
                    token_in: coverage.token1,
                    token_out: coverage.token0,
                },
            ]
        })
        .collect::<Vec<_>>();
    let mut cycles = Vec::new();
    for anchor in anchor_tokens {
        collect_anchor_cycles(
            *anchor,
            *anchor,
            &edges,
            &mut BTreeSet::new(),
            &mut Vec::new(),
            0,
            max_depth,
            &mut cycles,
        );
    }
    cycles.sort_by_key(|cycle| {
        (
            cycle.edges.len(),
            format!("{:#x}", cycle.anchor),
            cycle
                .edges
                .iter()
                .map(|edge| {
                    format!(
                        "{:#x}:{:#x}->{:#x}",
                        edge.pool, edge.token_in, edge.token_out
                    )
                })
                .collect::<Vec<_>>()
                .join("|"),
        )
    });
    cycles.dedup_by_key(|cycle| {
        (
            cycle.anchor,
            cycle
                .edges
                .iter()
                .map(|edge| (edge.pool, edge.token_in, edge.token_out))
                .collect::<Vec<_>>(),
        )
    });
    cycles
}

#[allow(clippy::too_many_arguments)]
fn collect_anchor_cycles(
    anchor: Address,
    current: Address,
    edges: &[CycleEdge],
    used_coverage: &mut BTreeSet<usize>,
    path: &mut Vec<CycleEdge>,
    depth: usize,
    max_depth: usize,
    cycles: &mut Vec<AnchorCycle>,
) {
    if depth >= max_depth {
        return;
    }
    for edge in edges {
        if edge.token_in != current || used_coverage.contains(&edge.coverage_index) {
            continue;
        }
        path.push(edge.clone());
        if edge.token_out == anchor && depth + 1 >= 2 {
            cycles.push(AnchorCycle {
                anchor,
                edges: path.clone(),
            });
            path.pop();
            continue;
        }
        used_coverage.insert(edge.coverage_index);
        collect_anchor_cycles(
            anchor,
            edge.token_out,
            edges,
            used_coverage,
            path,
            depth + 1,
            max_depth,
            cycles,
        );
        used_coverage.remove(&edge.coverage_index);
        path.pop();
    }
}

fn observed_pool_to_pool_anchor_amounts(
    receipt: &TxReceipt,
    coverages: &[PoolCoverage],
) -> BTreeMap<Address, U256> {
    let recognized_pools = coverages
        .iter()
        .map(|coverage| format!("{:#x}", coverage.pool).to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let anchor_tokens = coverages
        .iter()
        .flat_map(|coverage| [coverage.token0, coverage.token1])
        .collect::<BTreeSet<_>>();
    let mut max_by_token = BTreeMap::new();
    for log in receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if raw_log_topic0(log).as_deref() != Some(ERC20_TRANSFER_TOPIC) {
            continue;
        }
        let Some(token) = log
            .get("address")
            .and_then(Value::as_str)
            .and_then(|raw| Address::from_str(raw).ok())
        else {
            continue;
        };
        if !anchor_tokens.contains(&token) {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        if topics.len() < 3 {
            continue;
        }
        let Some(from) = topics[1].as_str().and_then(topic_address) else {
            continue;
        };
        let Some(to) = topics[2].as_str().and_then(topic_address) else {
            continue;
        };
        if !recognized_pools.contains(&from) || !recognized_pools.contains(&to) {
            continue;
        }
        let amount = parse_hex_u256(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        let current = max_by_token.entry(token).or_insert(U256::ZERO);
        if amount > *current {
            *current = amount;
        }
    }
    max_by_token
}

fn observed_cycle_anchor_input(receipt: &TxReceipt, cycle: &AnchorCycle) -> Option<U256> {
    let first_pool = cycle.edges.first()?.pool;
    let first_pool_text = format!("{first_pool:#x}").to_ascii_lowercase();
    let anchor_text = format!("{:#x}", cycle.anchor).to_ascii_lowercase();
    let mut best = U256::ZERO;
    for log in receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if raw_log_topic0(log).as_deref() != Some(ERC20_TRANSFER_TOPIC) {
            continue;
        }
        let Some(token) = log.get("address").and_then(Value::as_str) else {
            continue;
        };
        if token.to_ascii_lowercase() != anchor_text {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        if topics.len() < 3 {
            continue;
        }
        let Some(to) = topics[2].as_str().and_then(topic_address) else {
            continue;
        };
        if to != first_pool_text {
            continue;
        }
        let amount = parse_hex_u256(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        if amount > best {
            best = amount;
        }
    }
    (!best.is_zero()).then_some(best)
}

async fn load_enabled_pair_search_configs(pool: &PgPool) -> Result<Vec<TokenPairSearchConfig>> {
    let rows = sqlx::query_as::<_, TokenPairSearchConfigRow>(
        r#"
        SELECT
            tp.chain_id,
            tp.token0,
            tp.token1,
            tp.symbol,
            CASE
                WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                    THEN tp.token0_search_amounts
                ELSE COALESCE(token0_two_hop_default.search_amounts, token0_default.search_amounts)
            END AS token0_search_amounts,
            CASE
                WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                    THEN tp.token1_search_amounts
                ELSE COALESCE(token1_two_hop_default.search_amounts, token1_default.search_amounts)
            END AS token1_search_amounts,
            CASE
                WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                    THEN tp.token0_search_amounts
                ELSE COALESCE(token0_multihop_default.search_amounts, token0_default.search_amounts)
            END AS token0_multihop_search_amounts,
            CASE
                WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                    THEN tp.token1_search_amounts
                ELSE COALESCE(token1_multihop_default.search_amounts, token1_default.search_amounts)
            END AS token1_multihop_search_amounts,
            CASE
                WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                    THEN tp.token0_min_profit
                ELSE COALESCE(token0_two_hop_default.min_profit, token0_default.min_profit)
            END AS token0_min_profit,
            CASE
                WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                    THEN tp.token1_min_profit
                ELSE COALESCE(token1_two_hop_default.min_profit, token1_default.min_profit)
            END AS token1_min_profit,
            CASE
                WHEN NULLIF(BTRIM(tp.token0_search_amounts), '') IS NOT NULL
                    THEN tp.token0_min_profit
                ELSE COALESCE(token0_multihop_default.min_profit, token0_default.min_profit)
            END AS token0_multihop_min_profit,
            CASE
                WHEN NULLIF(BTRIM(tp.token1_search_amounts), '') IS NOT NULL
                    THEN tp.token1_min_profit
                ELSE COALESCE(token1_multihop_default.min_profit, token1_default.min_profit)
            END AS token1_multihop_min_profit,
            token0_default.min_profit AS token0_all_min_profit,
            token1_default.min_profit AS token1_all_min_profit
        FROM token_pairs tp
        LEFT JOIN token_search_defaults token0_default
          ON token0_default.chain_id = tp.chain_id
         AND token0_default.token_address = tp.token0
         AND token0_default.executor_scope = 'all'
        LEFT JOIN token_search_defaults token0_two_hop_default
          ON token0_two_hop_default.chain_id = tp.chain_id
         AND token0_two_hop_default.token_address = tp.token0
         AND token0_two_hop_default.executor_scope = 'two_hop'
        LEFT JOIN token_search_defaults token0_multihop_default
          ON token0_multihop_default.chain_id = tp.chain_id
         AND token0_multihop_default.token_address = tp.token0
         AND token0_multihop_default.executor_scope = 'multihop'
        LEFT JOIN token_search_defaults token1_default
          ON token1_default.chain_id = tp.chain_id
         AND token1_default.token_address = tp.token1
         AND token1_default.executor_scope = 'all'
        LEFT JOIN token_search_defaults token1_two_hop_default
          ON token1_two_hop_default.chain_id = tp.chain_id
         AND token1_two_hop_default.token_address = tp.token1
         AND token1_two_hop_default.executor_scope = 'two_hop'
        LEFT JOIN token_search_defaults token1_multihop_default
          ON token1_multihop_default.chain_id = tp.chain_id
         AND token1_multihop_default.token_address = tp.token1
         AND token1_multihop_default.executor_scope = 'multihop'
        WHERE tp.enabled = TRUE
          AND (
            COALESCE(
                NULLIF(BTRIM(tp.token0_search_amounts), ''),
                NULLIF(BTRIM(token0_two_hop_default.search_amounts), ''),
                NULLIF(BTRIM(token0_multihop_default.search_amounts), ''),
                NULLIF(BTRIM(token0_default.search_amounts), '')
            ) IS NOT NULL
            OR COALESCE(
                NULLIF(BTRIM(tp.token1_search_amounts), ''),
                NULLIF(BTRIM(token1_two_hop_default.search_amounts), ''),
                NULLIF(BTRIM(token1_multihop_default.search_amounts), ''),
                NULLIF(BTRIM(token1_default.search_amounts), '')
            ) IS NOT NULL
          )
        ORDER BY tp.updated_at DESC
        "#,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .filter_map(|row| match token_pair_config_from_row(row) {
            Ok(config)
                if !config.token0_search_amounts.is_empty()
                    || !config.token1_search_amounts.is_empty() =>
            {
                Some(Ok(config))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect()
}

fn token_pair_config_from_row(row: TokenPairSearchConfigRow) -> Result<TokenPairSearchConfig> {
    Ok(TokenPairSearchConfig {
        chain_id: u64::try_from(row.chain_id)?,
        token0: row.token0.parse()?,
        token1: row.token1.parse()?,
        symbol: row.symbol,
        token0_search_amounts: parse_raw_amount_list(row.token0_search_amounts.as_deref())?,
        token1_search_amounts: parse_raw_amount_list(row.token1_search_amounts.as_deref())?,
        token0_multihop_search_amounts: parse_raw_amount_list(
            row.token0_multihop_search_amounts.as_deref(),
        )?,
        token1_multihop_search_amounts: parse_raw_amount_list(
            row.token1_multihop_search_amounts.as_deref(),
        )?,
        token0_min_profit: effective_min_profit(
            row.token0_min_profit.as_deref(),
            row.token0_all_min_profit.as_deref(),
        )?,
        token1_min_profit: effective_min_profit(
            row.token1_min_profit.as_deref(),
            row.token1_all_min_profit.as_deref(),
        )?,
        token0_multihop_min_profit: effective_min_profit(
            row.token0_multihop_min_profit.as_deref(),
            row.token0_all_min_profit.as_deref(),
        )?,
        token1_multihop_min_profit: effective_min_profit(
            row.token1_multihop_min_profit.as_deref(),
            row.token1_all_min_profit.as_deref(),
        )?,
    })
}

fn effective_min_profit(raw: Option<&str>, all_raw: Option<&str>) -> Result<U256> {
    let min_profit = parse_raw_amount(raw)?.unwrap_or(U256::from(1u64));
    let all_min_profit = parse_raw_amount(all_raw)?.unwrap_or(U256::ZERO);
    Ok(min_profit.max(all_min_profit))
}

fn parse_raw_amount_list(raw: Option<&str>) -> Result<Vec<U256>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(parse_raw_amount(Some(trimmed)).and_then(|value| {
                    value.ok_or_else(|| anyhow::anyhow!("empty raw amount in list"))
                }))
            }
        })
        .collect()
}

fn parse_raw_amount(raw: Option<&str>) -> Result<Option<U256>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(U256::from_str_radix(raw, 10)?))
}

async fn load_pool_coverage(
    pool: &PgPool,
    chain_id: u64,
    block_number: u64,
    pool_address: Address,
    topic0: &str,
) -> Result<PoolCoverage> {
    let pool_text = format!("{pool_address:#x}").to_ascii_lowercase();
    let row = sqlx::query(
        r#"
        WITH target AS (SELECT lower($1::text) AS pool)
        SELECT
            COALESCE(p.token0, op.token0, po.token0) AS token0,
            COALESCE(p.token1, op.token1, po.token1) AS token1,
            COALESCE(tp.symbol, op.symbol, po.symbol) AS symbol,
            COALESCE(p.dex, op.dex, po.dex) AS dex,
            COALESCE(p.variant, op.variant, po.variant) AS variant,
            p.enabled AS enabled,
            COALESCE(p.factory_address, op.factory_address, po.factory_address) AS factory_address,
            COALESCE(op.fee_pips, po.fee_pips, p.fee_bps * 100) AS fee_pips,
            p.fee_bps AS fee_bps,
            COALESCE(p.tick_spacing, op.tick_spacing, po.tick_spacing) AS tick_spacing,
            p.stable AS stable,
            po.hooks_address AS hooks_address,
            ps.block_number AS latest_state_block,
            ps.source AS latest_state_source,
            (
                SELECT count(*)
                FROM opportunities o, target t
                WHERE o.block_number BETWEEN GREATEST($2 - 1, 0) AND $2 + 1
                  AND lower(o.path_json::text) LIKE '%' || t.pool || '%'
            ) AS opportunities_near_block
        FROM target t
        LEFT JOIN pools p ON p.chain_id = $3 AND lower(p.pool_address) = t.pool
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN observed_pools op ON op.chain_id = $3 AND lower(op.pool_address) = t.pool
        LEFT JOIN LATERAL (
            SELECT
                token0, token1, symbol, factory_address, dex, variant, fee_bps,
                fee_pips, tick_spacing, hooks_address
            FROM protocol_pool_observations po
            WHERE po.chain_id = $3
              AND lower(po.pool_address) = t.pool
            ORDER BY po.updated_at DESC
            LIMIT 1
        ) po ON true
        LEFT JOIN LATERAL (
            SELECT block_number, source
            FROM pool_states ps
            WHERE lower(ps.pool_address) = t.pool
            ORDER BY block_number DESC, updated_at DESC
            LIMIT 1
        ) ps ON true
        "#,
    )
    .bind(&pool_text)
    .bind(i64::try_from(block_number)?)
    .bind(i64::try_from(chain_id)?)
    .fetch_one(pool)
    .await?;

    let token0: String = row.try_get("token0")?;
    let token1: String = row.try_get("token1")?;
    let dex: String = row.try_get("dex")?;
    let variant: String = row.try_get("variant")?;
    let fee_pips: Option<i64> = row.try_get("fee_pips")?;
    let fee_bps: Option<i64> = row.try_get("fee_bps")?;
    let tick_spacing: Option<i64> = row.try_get("tick_spacing")?;
    let latest_state_block: Option<i64> = row.try_get("latest_state_block")?;
    Ok(PoolCoverage {
        pool: pool_address,
        topic0: topic0.to_string(),
        token0: Address::from_str(&token0).context("invalid token0")?,
        token1: Address::from_str(&token1).context("invalid token1")?,
        symbol: row
            .try_get::<Option<String>, _>("symbol")?
            .unwrap_or_else(|| format!("{}/{}", short_hex(&token0), short_hex(&token1))),
        dex: parse_dex(&dex)?,
        variant: parse_variant(&variant)?,
        fee_bps: fee_bps
            .or_else(|| fee_pips.map(|value| value / 100))
            .unwrap_or_default()
            .max(0) as u32,
        tick_spacing: tick_spacing.map(|value| value as i32),
        stable: row.try_get("stable")?,
        factory_address: row
            .try_get::<Option<String>, _>("factory_address")?
            .and_then(|raw| Address::from_str(&raw).ok()),
        hooks_address: row
            .try_get::<Option<String>, _>("hooks_address")?
            .and_then(|raw| Address::from_str(&raw).ok()),
        enabled: row.try_get::<Option<bool>, _>("enabled")?.unwrap_or(false),
        latest_state_block: latest_state_block.map(|value| value as u64),
        latest_state_source: row.try_get("latest_state_source")?,
        opportunities_near_block: row.try_get("opportunities_near_block")?,
    })
}

fn receipt_swap_logs(receipt: &TxReceipt) -> Vec<(Address, String)> {
    receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|log| {
            let topic0 = raw_log_topic0(log)?;
            if !is_swap_topic(&topic0) {
                return None;
            }
            let pool = if topic0 == UNISWAP_V4_SWAP_TOPIC {
                let pool_uid = log.get("topics")?.as_array()?.get(1)?.as_str()?;
                synthetic_address_from_pool_uid(pool_uid)?
            } else {
                Address::from_str(log.get("address")?.as_str()?).ok()?
            };
            Some((pool, topic0))
        })
        .collect()
}

fn dedupe_swap_pools(logs: Vec<(Address, String)>) -> Vec<(Address, String)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (pool, topic0) in logs {
        if seen.insert(pool) {
            out.push((pool, topic0));
        }
    }
    out
}

fn receipt_transfer_flow(receipt: &TxReceipt) -> Vec<String> {
    let mut transfers = Vec::new();
    for log in receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if raw_log_topic0(log).as_deref() != Some(ERC20_TRANSFER_TOPIC) {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        if topics.len() < 3 {
            continue;
        }
        let token = log.get("address").and_then(Value::as_str).unwrap_or("-");
        let from = topics[1].as_str().and_then(topic_address);
        let to = topics[2].as_str().and_then(topic_address);
        let amount = hex_to_decimal(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        transfers.push(format!(
            "token={} from={} to={} amount={}",
            short_hex(token),
            from.as_deref().map(short_hex).unwrap_or_else(|| "-".into()),
            to.as_deref().map(short_hex).unwrap_or_else(|| "-".into()),
            amount
        ));
    }
    if transfers.is_empty() {
        transfers.push("-".into());
    }
    transfers
}

fn receipt_topic_summary(receipt: &TxReceipt) -> String {
    let mut counts = BTreeMap::<String, usize>::new();
    for log in receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(topic0) = raw_log_topic0(log) else {
            continue;
        };
        let label = if topic0 == ERC20_TRANSFER_TOPIC {
            "erc20_transfer".into()
        } else if is_swap_topic(&topic0) {
            topic_family(&topic0).into()
        } else {
            format!("unknown:{}", short_hash(&topic0))
        };
        *counts.entry(label).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(label, count)| format!("{label}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn rough_quote_edge(
    edge: &CycleEdge,
    state: &PoolState,
    amount_in: U256,
) -> Result<U256, RoughQuoteFailure> {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            let reserve0 = state.reserve0.ok_or(RoughQuoteFailure::MissingReserves)?;
            let reserve1 = state.reserve1.ok_or(RoughQuoteFailure::MissingReserves)?;
            let (reserve_in, reserve_out) = if edge.token_in == state.token0 {
                (reserve0, reserve1)
            } else if edge.token_in == state.token1 {
                (reserve1, reserve0)
            } else {
                return Err(RoughQuoteFailure::TokenMismatch);
            };
            if state.stable.unwrap_or(false) {
                let decimals0 = state
                    .token0_decimals
                    .ok_or(RoughQuoteFailure::MissingDecimals)?;
                let decimals1 = state
                    .token1_decimals
                    .ok_or(RoughQuoteFailure::MissingDecimals)?;
                let (decimals_in, decimals_out) = if edge.token_in == state.token0 {
                    (decimals0, decimals1)
                } else {
                    (decimals1, decimals0)
                };
                AerodromeStableQuoter::quote_amount_out(
                    reserve_in,
                    reserve_out,
                    amount_in,
                    state.fee_bps,
                    decimals_in,
                    decimals_out,
                )
                .map_err(|_| RoughQuoteFailure::StableQuoteFailed)
            } else {
                AerodromeVolatileQuoter::quote_amount_out(
                    reserve_in,
                    reserve_out,
                    amount_in,
                    state.fee_bps,
                )
                .map_err(|_| RoughQuoteFailure::V2QuoteFailed)
            }
        }
        PoolVariant::AerodromeSlipstream
        | PoolVariant::UniswapV3
        | PoolVariant::PancakeV3
        | PoolVariant::UniswapV4 => spot_quote_exact_in(state, edge.token_in, amount_in)
            .map_err(classify_v3_spot_quote_failure),
        PoolVariant::BalancerV3 => {
            if state.balancer_model.as_deref() == Some("weighted") {
                quote_balancer_weighted_exact_in(state, edge.token_in, edge.token_out, amount_in)
                    .map_err(|_| RoughQuoteFailure::BalancerQuoteFailed)
            } else {
                Err(RoughQuoteFailure::UnsupportedPool)
            }
        }
    }
}

fn classify_v3_spot_quote_failure(err: base_arb_common::errors::ArbBotError) -> RoughQuoteFailure {
    let message = err.to_string();
    if message.contains("token_in not in pool") {
        RoughQuoteFailure::TokenMismatch
    } else if message.contains("missing sqrt_price_x96") || message.contains("empty sqrt_price_x96")
    {
        RoughQuoteFailure::MissingV3State
    } else if message.contains("overflow") {
        RoughQuoteFailure::V3SpotOverflow
    } else {
        RoughQuoteFailure::V3SpotQuoteFailed
    }
}

fn estimate_price_impact_bps(
    pool_state: &PoolState,
    token_in: Address,
    quote: &QuoteResult,
) -> u64 {
    if matches!(pool_state.variant, PoolVariant::BalancerV3) {
        return 0;
    }
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
        return 0;
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

fn quote_model_edge_floor(
    amount_in: U256,
    diagnostics: &QuoteDiagnostics,
    v3_quote_safety_bps: u64,
) -> U256 {
    let v3_steps = diagnostics
        .steps
        .iter()
        .filter(|step| is_v3_style_variant(step.variant))
        .count() as u64;
    if amount_in.is_zero() || v3_steps == 0 || v3_quote_safety_bps == 0 {
        return U256::ZERO;
    }
    amount_in.saturating_mul(U256::from(v3_steps.saturating_mul(v3_quote_safety_bps)))
        / U256::from(10_000u64)
}

fn tick_repair_pools_from_diagnostics(diagnostics: &QuoteDiagnostics) -> Vec<Address> {
    let mut seen = HashSet::new();
    diagnostics
        .steps
        .iter()
        .filter(|step| step.tick_range_exhausted && is_v3_style_variant(step.variant))
        .filter_map(|step| seen.insert(step.pool).then_some(step.pool))
        .collect()
}

fn apply_quote_haircut(amount: U256, haircut_bps: u64) -> Result<U256> {
    if haircut_bps == 0 || amount.is_zero() {
        return Ok(amount);
    }
    let denominator = U256::from(10_000u64);
    let numerator = denominator
        .checked_sub(U256::from(haircut_bps.min(10_000)))
        .context("invalid V3 quote haircut")?;
    amount
        .checked_mul(numerator)
        .and_then(|value| value.checked_div(denominator))
        .context("V3 quote haircut overflow")
}

fn expand_max_amounts(max_amounts: &[U256]) -> Vec<U256> {
    let denominator = U256::from(MAX_AMOUNT_TIER_DENOMINATOR);
    let mut out = Vec::new();
    for max_amount in max_amounts
        .iter()
        .copied()
        .filter(|amount| !amount.is_zero())
    {
        for numerator in MAX_AMOUNT_TIER_NUMERATORS {
            let amount = max_amount
                .saturating_mul(U256::from(*numerator))
                .checked_div(denominator)
                .unwrap_or(U256::ZERO);
            if !amount.is_zero() {
                out.push(amount);
            }
        }
        out.push(max_amount);
    }
    out.sort();
    out.dedup();
    out
}

fn pool_depth_score(state: &PoolState) -> U256 {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            state.reserve0.unwrap_or_default() + state.reserve1.unwrap_or_default()
        }
        PoolVariant::AerodromeSlipstream
        | PoolVariant::UniswapV3
        | PoolVariant::PancakeV3
        | PoolVariant::UniswapV4 => state.liquidity.unwrap_or_default(),
        PoolVariant::BalancerV3 => U256::from(1u64),
    }
}

fn is_pool_state_active(
    state: &PoolState,
    now: DateTime<Utc>,
    max_pool_state_age_ms: i64,
    settings: &Settings,
) -> bool {
    if state.is_stale(now, max_pool_state_age_ms) {
        return false;
    }
    is_pool_state_quote_ready(state, settings)
}

fn is_pool_state_quote_ready(state: &PoolState, settings: &Settings) -> bool {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            is_supported_factory(
                state,
                settings.aerodrome_pool_factory,
                &[AERODROME_CLASSIC_FACTORY, UNISWAP_V2_FACTORY],
            ) && is_nonzero_u256(state.reserve0)
                && is_nonzero_u256(state.reserve1)
        }
        PoolVariant::AerodromeSlipstream => {
            is_supported_factory(
                state,
                settings.aerodrome_slipstream_factory,
                &AERODROME_SLIPSTREAM_FACTORIES,
            ) && state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::UniswapV3 => {
            is_supported_factory(state, settings.uniswap_v3_factory, &[UNISWAP_V3_FACTORY])
                && state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::PancakeV3 => {
            is_supported_factory(state, settings.pancake_v3_factory, &[PANCAKE_V3_FACTORY])
                && state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::UniswapV4 => {
            is_supported_manager(state, settings.uniswap_v4_pool_manager)
                && state.sqrt_price_x96.is_some()
                && is_nonzero_u256(state.liquidity)
                && state.tick.is_some()
        }
        PoolVariant::BalancerV3 => {
            is_supported_manager(state, settings.balancer_v3_vault)
                && state.token0 != Address::ZERO
                && state.token1 != Address::ZERO
        }
    }
}

fn is_supported_pool(state: &PoolState) -> bool {
    state.token0 != Address::ZERO && state.token1 != Address::ZERO && state.token0 != state.token1
}

fn is_supported_factory(
    state: &PoolState,
    configured: Option<Address>,
    fallback_supported: &[&str],
) -> bool {
    let Some(factory) = state.factory_address else {
        return false;
    };
    if configured == Some(factory) {
        return true;
    }
    fallback_supported
        .iter()
        .any(|expected| Address::from_str(expected).is_ok_and(|expected| expected == factory))
}

fn is_supported_manager(state: &PoolState, configured: Option<Address>) -> bool {
    state
        .factory_address
        .is_some_and(|factory| configured == Some(factory))
}

fn is_nonzero_u256(value: Option<U256>) -> bool {
    value.is_some_and(|value| !value.is_zero())
}

fn is_v3_style_variant(variant: PoolVariant) -> bool {
    matches!(
        variant,
        PoolVariant::AerodromeSlipstream
            | PoolVariant::UniswapV3
            | PoolVariant::PancakeV3
            | PoolVariant::UniswapV4
    )
}

impl QuoteSkip {
    fn new(reason: QuoteSkipReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            repair_pools: Vec::new(),
        }
    }

    fn with_repair_pool(
        reason: QuoteSkipReason,
        message: impl Into<String>,
        pool: Address,
    ) -> Self {
        Self {
            reason,
            message: message.into(),
            repair_pools: vec![pool],
        }
    }

    fn with_repair_pools(
        reason: QuoteSkipReason,
        message: impl Into<String>,
        repair_pools: Vec<Address>,
    ) -> Self {
        Self {
            reason,
            message: message.into(),
            repair_pools,
        }
    }

    fn quote_error(message: impl Into<String>) -> Self {
        Self::new(QuoteSkipReason::QuoteError, message)
    }
}

fn parse_args<I>(args: I, settings: &Settings) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut tx_hash = None;
    let mut output = None;
    let mut max_depth = 4usize;
    let mut max_price_impact_bps = settings.max_price_impact_bps;
    let mut max_pool_state_age_ms = settings.max_pool_state_age_ms;
    let mut min_expected_profit = usdc_to_units(settings.min_expected_profit_usdc);
    let mut shadow_token_amounts: HashMap<Address, Vec<U256>> = HashMap::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--tx-hash" | "--tx" => {
                let raw = args.next().context("--tx-hash requires a value")?;
                tx_hash = Some(B256::from_str(&raw).context("invalid --tx-hash")?);
            }
            "--report-entry" => {
                let raw = args.next().context("--report-entry requires a value")?;
                let hash = extract_tx_hash(&raw)
                    .with_context(|| format!("no tx hash found in report entry: {raw}"))?;
                tx_hash = Some(B256::from_str(&hash).context("invalid report-entry tx hash")?);
            }
            "--output" | "--out" => {
                output = Some(PathBuf::from(
                    args.next().context("--output requires a value")?,
                ));
            }
            "--max-depth" => {
                max_depth = args
                    .next()
                    .context("--max-depth requires a value")?
                    .parse()
                    .context("invalid --max-depth")?;
            }
            "--max-price-impact-bps" => {
                max_price_impact_bps = args
                    .next()
                    .context("--max-price-impact-bps requires a value")?
                    .parse()
                    .context("invalid --max-price-impact-bps")?;
            }
            "--max-pool-state-age-ms" => {
                max_pool_state_age_ms = args
                    .next()
                    .context("--max-pool-state-age-ms requires a value")?
                    .parse()
                    .context("invalid --max-pool-state-age-ms")?;
            }
            "--min-expected-profit" => {
                min_expected_profit = U256::from_str_radix(
                    &args
                        .next()
                        .context("--min-expected-profit requires a value")?,
                    10,
                )
                .context("invalid --min-expected-profit")?;
            }
            "--shadow-token-amount" => {
                let raw = args
                    .next()
                    .context("--shadow-token-amount requires TOKEN:RAW_AMOUNT")?;
                let (token, amount) = parse_shadow_token_amount(&raw, settings)
                    .with_context(|| format!("invalid --shadow-token-amount {raw}"))?;
                if !amount.is_zero() {
                    shadow_token_amounts.entry(token).or_default().push(amount);
                }
            }
            "--shadow-token-max" => {
                let raw = args
                    .next()
                    .context("--shadow-token-max requires TOKEN:RAW_MAX_AMOUNT")?;
                let (token, amount) = parse_shadow_token_amount(&raw, settings)
                    .with_context(|| format!("invalid --shadow-token-max {raw}"))?;
                let expanded = expand_max_amounts(&[amount]);
                shadow_token_amounts
                    .entry(token)
                    .or_default()
                    .extend(expanded);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    for amounts in shadow_token_amounts.values_mut() {
        amounts.sort();
        amounts.dedup();
    }
    Ok(Cli {
        tx_hash: tx_hash.context("--tx-hash or --report-entry is required")?,
        output,
        max_depth,
        max_price_impact_bps,
        max_pool_state_age_ms,
        min_expected_profit,
        shadow_token_amounts,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_searcher_pipeline_diag -- --tx-hash <0x...> [--output /tmp/report.txt] [--shadow-token-max USDC:<raw>] [--shadow-token-max WETH:<raw>]"
    );
}

fn parse_shadow_token_amount(raw: &str, settings: &Settings) -> Result<(Address, U256)> {
    let (token_raw, amount_raw) = raw.split_once(':').context("expected TOKEN:RAW_AMOUNT")?;
    let token = parse_token_alias(token_raw, settings)?;
    let amount = U256::from_str_radix(amount_raw, 10).context("invalid raw amount")?;
    Ok((token, amount))
}

fn parse_token_alias(raw: &str, settings: &Settings) -> Result<Address> {
    if raw.eq_ignore_ascii_case("USDC") {
        return Ok(settings.usdc_address);
    }
    if raw.eq_ignore_ascii_case("WETH") {
        return Ok(settings.weth_address);
    }
    Address::from_str(raw).context("invalid token alias/address")
}

fn extract_tx_hash(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    for index in 0..bytes.len().saturating_sub(65) {
        if bytes[index] == b'0'
            && matches!(bytes.get(index + 1), Some(b'x') | Some(b'X'))
            && raw
                .get(index + 2..index + 66)
                .is_some_and(|candidate| candidate.chars().all(|ch| ch.is_ascii_hexdigit()))
        {
            return raw.get(index..index + 66).map(str::to_string);
        }
    }
    None
}

fn parse_dex(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        "UniswapV4" => Ok(DexKind::UniswapV4),
        "Balancer" => Ok(DexKind::Balancer),
        _ => bail!("unknown dex kind {value}"),
    }
}

fn parse_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        "UniswapV4" => Ok(PoolVariant::UniswapV4),
        "BalancerV3" => Ok(PoolVariant::BalancerV3),
        _ => bail!("unknown pool variant {value}"),
    }
}

fn is_swap_topic(topic0: &str) -> bool {
    matches!(
        topic0,
        UNI_V3_SWAP_TOPIC
            | PANCAKE_V3_SWAP_TOPIC
            | CLASSIC_SWAP_TOPIC
            | AERODROME_CLASSIC_SWAP_TOPIC
            | UNISWAP_V4_SWAP_TOPIC
    )
}

fn topic_family(topic0: &str) -> &'static str {
    match topic0 {
        UNI_V3_SWAP_TOPIC => "uni_v3",
        PANCAKE_V3_SWAP_TOPIC => "pancake_v3",
        CLASSIC_SWAP_TOPIC => "classic",
        AERODROME_CLASSIC_SWAP_TOPIC => "aero_classic",
        UNISWAP_V4_SWAP_TOPIC => "uniswap_v4",
        _ => "unknown",
    }
}

fn raw_log_topic0(log: &Value) -> Option<String> {
    log.get("topics")?
        .as_array()?
        .first()?
        .as_str()
        .map(|value| value.to_ascii_lowercase())
}

fn topic_address(topic: &str) -> Option<String> {
    let raw = topic.strip_prefix("0x").unwrap_or(topic);
    if raw.len() != 64 {
        return None;
    }
    Some(format!("0x{}", &raw[24..64]).to_ascii_lowercase())
}

fn synthetic_address_from_pool_uid(pool_uid: &str) -> Option<Address> {
    let raw = pool_uid.strip_prefix("0x").unwrap_or(pool_uid);
    if raw.len() < 40 {
        return None;
    }
    Address::from_str(&format!("0x{}", &raw[raw.len() - 40..])).ok()
}

fn parse_hex_u256(raw: &str) -> U256 {
    U256::from_str_radix(raw.trim_start_matches("0x"), 16).unwrap_or(U256::ZERO)
}

fn hex_to_decimal(raw: &str) -> String {
    parse_hex_u256(raw).to_string()
}

fn usdc_to_units(usdc: f64) -> U256 {
    U256::from((usdc * 1_000_000.0) as u64)
}

fn short_hash(value: &str) -> String {
    let raw = value.strip_prefix("0x").unwrap_or(value);
    if raw.len() <= 16 {
        return value.to_string();
    }
    format!("0x{}..{}", &raw[..8], &raw[raw.len() - 6..])
}

fn short_addr(value: Address) -> String {
    short_hex(format!("{value:#x}"))
}

fn short_hex(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    let raw = value.strip_prefix("0x").unwrap_or(value);
    if raw.len() < 12 {
        return value.to_string();
    }
    format!("0x{}..{}", &raw[..6], &raw[raw.len() - 6..])
}

fn display_shadow_token_amounts(amounts_by_token: &HashMap<Address, Vec<U256>>) -> String {
    if amounts_by_token.is_empty() {
        return "-".into();
    }
    let mut rows = amounts_by_token
        .iter()
        .map(|(token, amounts)| {
            format!(
                "{token:#x}:{}",
                amounts
                    .iter()
                    .map(U256::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows.join(" | ")
}

fn display_opt_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}
