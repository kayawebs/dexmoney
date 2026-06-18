use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
};

use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::{ChainProvider, TxReceipt};
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::{json, Value};
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

#[derive(Debug)]
struct Cli {
    collector: Address,
    lookback_blocks: u64,
    limit: usize,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CollectorTx {
    tx_hash: B256,
    block_number: u64,
}

#[derive(Debug)]
struct PoolCoverage {
    pool: String,
    topic0: String,
    token0: Option<String>,
    token1: Option<String>,
    symbol: Option<String>,
    dex: Option<String>,
    variant: Option<String>,
    pool_enabled: Option<bool>,
    latest_state_block: Option<i64>,
    latest_state_source: Option<String>,
    observed_status: Option<String>,
    discovery_source: Option<String>,
    dex_event_logs_same_block: i64,
    opportunities_near_block: i64,
}

#[derive(Debug)]
struct TxDiagnostics {
    topic_summary: String,
    transfer_flow: Vec<String>,
    unrecognized_transfer_counterparties: Vec<String>,
    anchor_input_guesses: Vec<String>,
    recognized_swap_pools: usize,
    anchors_touched: Vec<String>,
    recognized_anchor_cycle: bool,
    anchor_cycles: Vec<String>,
    searcher_gap: String,
}

#[derive(Debug, Default, Clone)]
struct AnchorAmountConfig {
    all_amounts: Option<String>,
    all_min_profit: Option<String>,
    multihop_amounts: Option<String>,
    multihop_min_profit: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let provider = ChainProvider::from_settings(&settings);

    let latest_block = provider.get_block_number().await?;
    let from_block = latest_block.saturating_sub(cli.lookback_blocks);
    let txs = load_collector_txs(
        &provider,
        cli.collector,
        from_block,
        latest_block,
        cli.limit,
    )
    .await
    .context("failed to load collector transfer transactions")?;

    let out_path = cli.output.unwrap_or_else(|| {
        PathBuf::from(format!(
            "reports/competitor-live-compare-{}-{}.txt",
            from_block, latest_block
        ))
    });
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let mut writer = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?,
    );

    writeln!(writer, "competitor live compare")?;
    writeln!(writer, "collector: {:#x}", cli.collector)?;
    writeln!(writer, "from_block: {from_block}")?;
    writeln!(writer, "to_block: {latest_block}")?;
    writeln!(writer, "collector_transfer_txs: {}", txs.len())?;
    let anchor_configs = load_anchor_configs(&store.pool).await?;
    let anchor_tokens = anchor_configs.keys().cloned().collect::<BTreeSet<_>>();
    writeln!(
        writer,
        "anchor_tokens: {}",
        anchor_tokens
            .iter()
            .map(|token| short_addr(token))
            .collect::<Vec<_>>()
            .join(", ")
    )?;
    writeln!(writer)?;

    let mut summary = BTreeMap::<String, usize>::new();
    let mut gap_summary = BTreeMap::<String, usize>::new();
    let mut unrecognized_counterparty_summary = BTreeMap::<String, usize>::new();
    for tx in txs {
        let Some(receipt) = provider.get_transaction_receipt(tx.tx_hash).await? else {
            *summary.entry("receipt_missing".into()).or_default() += 1;
            continue;
        };
        let swap_logs = receipt_swap_logs(&receipt);
        if swap_logs.is_empty() {
            *summary.entry("no_swap_logs".into()).or_default() += 1;
            continue;
        }
        let profits = collector_inbound_transfers(&receipt, cli.collector);
        let mut coverages = Vec::new();
        for (pool, topic0) in dedupe_swap_pools(swap_logs) {
            coverages.push(
                load_pool_coverage(
                    &store.pool,
                    settings.chain_id,
                    tx.block_number,
                    &pool,
                    &topic0,
                )
                .await?,
            );
        }
        let classification = classify_tx(&coverages);
        let diagnostics = tx_diagnostics(
            &receipt,
            cli.collector,
            &coverages,
            &anchor_tokens,
            &anchor_configs,
        );
        *summary.entry(classification.clone()).or_default() += 1;
        *gap_summary
            .entry(diagnostics.searcher_gap.clone())
            .or_default() += 1;
        for counterparty in &diagnostics.unrecognized_transfer_counterparties {
            *unrecognized_counterparty_summary
                .entry(counterparty.clone())
                .or_default() += 1;
        }
        write_tx(
            &mut writer,
            &tx,
            &receipt,
            &profits,
            &coverages,
            &diagnostics,
            &classification,
        )?;
    }

    writeln!(writer)?;
    writeln!(writer, "== Summary ==")?;
    for (classification, n) in summary {
        writeln!(writer, "{classification}\t{n}")?;
    }
    writeln!(writer)?;
    writeln!(writer, "== Searcher Gap Summary ==")?;
    for (gap, n) in gap_summary {
        writeln!(writer, "{gap}\t{n}")?;
    }
    writeln!(writer)?;
    writeln!(writer, "== Unrecognized Counterparties ==")?;
    let mut counterparties = unrecognized_counterparty_summary
        .into_iter()
        .collect::<Vec<_>>();
    counterparties.sort_by(|(left_address, left_count), (right_address, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_address.cmp(right_address))
    });
    for (address, n) in counterparties {
        writeln!(writer, "{}\t{}", address, n)?;
    }
    writer.flush()?;

    println!("wrote {}", out_path.display());
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut collector = None;
    let mut lookback_blocks = 500u64;
    let mut limit = 50usize;
    let mut output = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--address" | "--collector" => {
                let raw = args.next().context("--address requires a value")?;
                collector = Some(Address::from_str(&raw).context("invalid collector address")?);
            }
            "--lookback-blocks" => {
                lookback_blocks = args
                    .next()
                    .context("--lookback-blocks requires a value")?
                    .parse()
                    .context("invalid --lookback-blocks")?;
            }
            "--limit" => {
                limit = args
                    .next()
                    .context("--limit requires a value")?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--output" | "--out" => {
                output = Some(PathBuf::from(
                    args.next().context("--output requires a value")?,
                ));
            }
            "--help" | "-h" => {
                println!("Usage: cargo run -p base-arb-recorder --bin competitor_live_compare -- --address <collector> [--lookback-blocks 500] [--limit 50] [--output report.txt]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(Cli {
        collector: collector.context("--address is required")?,
        lookback_blocks,
        limit,
        output,
    })
}

async fn load_collector_txs(
    provider: &ChainProvider,
    collector: Address,
    from_block: u64,
    to_block: u64,
    limit: usize,
) -> Result<Vec<CollectorTx>> {
    let params = json!([{
        "fromBlock": format!("0x{from_block:x}"),
        "toBlock": format!("0x{to_block:x}"),
        "topics": [ERC20_TRANSFER_TOPIC, null, address_topic(collector)]
    }]);
    let logs = provider.get_logs_raw(params).await?;
    let mut by_tx = BTreeMap::<B256, u64>::new();
    for log in logs {
        let Some(tx_hash) = raw_log_tx_hash(&log) else {
            continue;
        };
        let block_number = raw_log_block_number(&log).unwrap_or_default();
        by_tx.entry(tx_hash).or_insert(block_number);
    }
    let mut txs = by_tx
        .into_iter()
        .map(|(tx_hash, block_number)| CollectorTx {
            tx_hash,
            block_number,
        })
        .collect::<Vec<_>>();
    txs.sort_by(|left, right| {
        right
            .block_number
            .cmp(&left.block_number)
            .then_with(|| right.tx_hash.cmp(&left.tx_hash))
    });
    txs.truncate(limit);
    Ok(txs)
}

fn receipt_swap_logs(receipt: &TxReceipt) -> Vec<(String, String)> {
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
            let pool = log.get("address")?.as_str()?.to_ascii_lowercase();
            Some((pool, topic0))
        })
        .collect()
}

fn dedupe_swap_pools(logs: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (pool, topic0) in logs {
        if seen.insert(pool.clone()) {
            out.push((pool, topic0));
        }
    }
    out
}

fn collector_inbound_transfers(receipt: &TxReceipt, collector: Address) -> Vec<String> {
    let collector_topic = address_topic(collector);
    receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|log| {
            let topic0 = raw_log_topic0(log)?;
            if topic0 != ERC20_TRANSFER_TOPIC {
                return None;
            }
            let topics = log.get("topics")?.as_array()?;
            if topics.len() < 3 || topics[2].as_str()?.to_ascii_lowercase() != collector_topic {
                return None;
            }
            let token = log.get("address")?.as_str()?.to_ascii_lowercase();
            let amount = hex_to_decimal(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
            Some(format!("{} {}", short_addr(&token), amount))
        })
        .collect()
}

async fn load_pool_coverage(
    pool: &PgPool,
    chain_id: u64,
    block_number: u64,
    pool_address: &str,
    topic0: &str,
) -> Result<PoolCoverage> {
    let row = sqlx::query(
        r#"
        WITH target AS (SELECT lower($1::text) AS pool)
        SELECT
            COALESCE(p.token0, op.token0) AS token0,
            COALESCE(p.token1, op.token1) AS token1,
            COALESCE(tp.symbol, op.symbol) AS symbol,
            p.dex,
            p.variant,
            p.enabled AS pool_enabled,
            ps.block_number AS latest_state_block,
            ps.source AS latest_state_source,
            op.import_status AS observed_status,
            op.discovery_source,
            (
                SELECT count(*)
                FROM dex_events e, target t
                WHERE lower(e.pool_address) = t.pool
                  AND e.block_number = $2
            ) AS dex_event_logs_same_block,
            (
                SELECT count(*)
                FROM opportunities o, target t
                WHERE o.block_number BETWEEN GREATEST($2 - 1, 0) AND $2 + 1
                  AND lower(o.path_json::text) LIKE '%' || t.pool || '%'
            ) AS opportunities_near_block
        FROM target t
        LEFT JOIN pools p ON lower(p.pool_address) = t.pool
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN LATERAL (
            SELECT block_number, source
            FROM pool_states ps
            WHERE lower(ps.pool_address) = t.pool
            ORDER BY block_number DESC
            LIMIT 1
        ) ps ON true
        LEFT JOIN observed_pools op
            ON op.chain_id = $3 AND lower(op.pool_address) = t.pool
        "#,
    )
    .bind(pool_address)
    .bind(i64::try_from(block_number)?)
    .bind(i64::try_from(chain_id)?)
    .fetch_one(pool)
    .await?;

    Ok(PoolCoverage {
        pool: pool_address.to_string(),
        topic0: topic0.to_string(),
        token0: row.try_get("token0")?,
        token1: row.try_get("token1")?,
        symbol: row.try_get("symbol")?,
        dex: row.try_get("dex")?,
        variant: row.try_get("variant")?,
        pool_enabled: row.try_get("pool_enabled")?,
        latest_state_block: row.try_get("latest_state_block")?,
        latest_state_source: row.try_get("latest_state_source")?,
        observed_status: row.try_get("observed_status")?,
        discovery_source: row.try_get("discovery_source")?,
        dex_event_logs_same_block: row.try_get("dex_event_logs_same_block")?,
        opportunities_near_block: row.try_get("opportunities_near_block")?,
    })
}

async fn load_anchor_configs(pool: &PgPool) -> Result<BTreeMap<String, AnchorAmountConfig>> {
    let rows = sqlx::query(
        r#"
        SELECT
            lower(token_address) AS token,
            executor_scope,
            search_amounts,
            min_profit
        FROM token_search_defaults
        WHERE COALESCE(search_amounts, '') <> ''
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut configs = BTreeMap::new();
    for row in rows {
        let token: String = row.try_get("token")?;
        let scope: String = row.try_get("executor_scope")?;
        let search_amounts: Option<String> = row.try_get("search_amounts")?;
        let min_profit: Option<String> = row.try_get("min_profit")?;
        let config: &mut AnchorAmountConfig = configs.entry(token).or_default();
        match scope.as_str() {
            "all" => {
                config.all_amounts = search_amounts;
                config.all_min_profit = min_profit;
            }
            "multihop" => {
                config.multihop_amounts = search_amounts;
                config.multihop_min_profit = min_profit;
            }
            _ => {}
        }
    }
    Ok(configs)
}

fn classify_tx(coverages: &[PoolCoverage]) -> String {
    if coverages.iter().any(|row| row.symbol.is_none()) {
        return "missing_pool_registry".into();
    }
    if coverages.iter().any(|row| row.pool_enabled != Some(true)) {
        return "pool_disabled".into();
    }
    if coverages.iter().any(|row| row.latest_state_block.is_none()) {
        return "missing_pool_state".into();
    }
    if coverages
        .iter()
        .all(|row| row.opportunities_near_block == 0)
    {
        return "covered_but_no_opportunity_near_block".into();
    }
    "opportunity_existed_near_block".into()
}

fn tx_diagnostics(
    receipt: &TxReceipt,
    collector: Address,
    coverages: &[PoolCoverage],
    anchor_tokens: &BTreeSet<String>,
    anchor_configs: &BTreeMap<String, AnchorAmountConfig>,
) -> TxDiagnostics {
    let recognized_swap_pools = coverages.len();
    let anchors_touched = anchors_touched(coverages, anchor_tokens);
    let anchor_cycles = anchor_cycles(coverages, anchor_tokens, 4);
    let recognized_anchor_cycle = !anchor_cycles.is_empty();
    let has_opportunity = coverages.iter().any(|row| row.opportunities_near_block > 0);
    let searcher_gap = if recognized_swap_pools == 0 {
        "no_supported_swap_logs"
    } else if !recognized_anchor_cycle {
        "recognized_swaps_do_not_form_anchor_cycle"
    } else if !has_opportunity {
        "recognized_anchor_cycle_but_no_opportunity"
    } else {
        "opportunity_existed_near_block"
    }
    .to_string();

    TxDiagnostics {
        topic_summary: receipt_topic_summary(receipt),
        transfer_flow: receipt_transfer_flow(receipt, collector),
        unrecognized_transfer_counterparties: unrecognized_transfer_counterparties(
            receipt, collector, coverages,
        ),
        anchor_input_guesses: anchor_input_guesses(
            receipt,
            collector,
            anchor_tokens,
            anchor_configs,
        ),
        recognized_swap_pools,
        anchors_touched,
        recognized_anchor_cycle,
        anchor_cycles,
        searcher_gap,
    }
}

fn anchors_touched(coverages: &[PoolCoverage], anchor_tokens: &BTreeSet<String>) -> Vec<String> {
    let mut touched = BTreeSet::new();
    for row in coverages {
        for token in [&row.token0, &row.token1].into_iter().flatten() {
            let token = token.to_ascii_lowercase();
            if anchor_tokens.contains(&token) {
                touched.insert(token);
            }
        }
    }
    touched.into_iter().collect()
}

fn anchor_cycles(
    coverages: &[PoolCoverage],
    anchor_tokens: &BTreeSet<String>,
    max_depth: usize,
) -> Vec<String> {
    let edges = coverages
        .iter()
        .enumerate()
        .filter_map(|(idx, row)| {
            let token0 = row.token0.as_ref()?.to_ascii_lowercase();
            let token1 = row.token1.as_ref()?.to_ascii_lowercase();
            Some((idx, row.pool.clone(), row.symbol.clone(), token0, token1))
        })
        .collect::<Vec<_>>();

    let mut cycles = Vec::new();
    for anchor in anchor_tokens {
        if !edges
            .iter()
            .any(|(_, _, _, token0, token1)| token0 == anchor || token1 == anchor)
        {
            continue;
        }
        collect_anchor_cycles(
            anchor,
            anchor,
            &edges,
            &mut BTreeSet::new(),
            &mut Vec::new(),
            0,
            max_depth,
            &mut cycles,
        );
    }
    cycles.sort();
    cycles.dedup();
    cycles.truncate(10);
    cycles
}

fn collect_anchor_cycles(
    anchor: &str,
    current: &str,
    edges: &[(usize, String, Option<String>, String, String)],
    used_edges: &mut BTreeSet<usize>,
    path: &mut Vec<String>,
    depth: usize,
    max_depth: usize,
    cycles: &mut Vec<String>,
) {
    if depth >= max_depth {
        return;
    }

    for (idx, pool, symbol, token0, token1) in edges {
        if used_edges.contains(idx) {
            continue;
        }
        let next = if token0 == current {
            token1
        } else if token1 == current {
            token0
        } else {
            continue;
        };

        path.push(format!(
            "{}:{}({}->{})",
            pool,
            symbol.as_deref().unwrap_or("-"),
            current,
            next
        ));
        if next == anchor && depth + 1 >= 2 {
            cycles.push(path.join(" | "));
            path.pop();
            continue;
        }

        used_edges.insert(*idx);
        collect_anchor_cycles(
            anchor,
            next,
            edges,
            used_edges,
            path,
            depth + 1,
            max_depth,
            cycles,
        );
        used_edges.remove(idx);
        path.pop();
    }
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

    if counts.is_empty() {
        return "-".into();
    }
    counts
        .into_iter()
        .map(|(label, count)| format!("{label}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn receipt_transfer_flow(receipt: &TxReceipt, collector: Address) -> Vec<String> {
    let collector_address = format!("{:#x}", collector).to_ascii_lowercase();
    let mut transfers = Vec::new();
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
        if topic0 != ERC20_TRANSFER_TOPIC {
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
        let token = log
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_ascii_lowercase();
        let amount = hex_to_decimal(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        let collector_tag = if to == collector_address {
            " collector_in"
        } else if from == collector_address {
            " collector_out"
        } else {
            ""
        };
        transfers.push(format!(
            "token={} from={} to={} amount={}{}",
            short_addr(&token),
            short_addr(&from),
            short_addr(&to),
            amount,
            collector_tag
        ));
    }

    if transfers.is_empty() {
        transfers.push("-".into());
    }
    transfers
}

fn unrecognized_transfer_counterparties(
    receipt: &TxReceipt,
    collector: Address,
    coverages: &[PoolCoverage],
) -> Vec<String> {
    let collector_address = format!("{:#x}", collector).to_ascii_lowercase();
    let mut known = coverages
        .iter()
        .map(|coverage| coverage.pool.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    known.insert(collector_address.clone());
    known.insert("0x0000000000000000000000000000000000000000".into());

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
        if topic0 != ERC20_TRANSFER_TOPIC {
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
        if to == collector_address {
            // This is usually the competitor's executor/settlement contract.
            known.insert(from);
        }
    }

    let mut out = BTreeSet::new();
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
        if topic0 != ERC20_TRANSFER_TOPIC {
            continue;
        }
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        if topics.len() < 3 {
            continue;
        }
        for topic in topics.iter().skip(1).take(2) {
            let Some(address) = topic.as_str().and_then(topic_address) else {
                continue;
            };
            if !known.contains(&address) {
                out.insert(address);
            }
        }
    }

    out.into_iter().collect()
}

fn anchor_input_guesses(
    receipt: &TxReceipt,
    collector: Address,
    anchor_tokens: &BTreeSet<String>,
    anchor_configs: &BTreeMap<String, AnchorAmountConfig>,
) -> Vec<String> {
    let collector_address = format!("{:#x}", collector).to_ascii_lowercase();
    let mut executor_addresses = BTreeSet::new();
    let mut profit_by_token = BTreeMap::<String, U256>::new();
    let mut input_by_token = BTreeMap::<String, U256>::new();

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
        if topic0 != ERC20_TRANSFER_TOPIC {
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
        if to != collector_address {
            continue;
        }
        let token = log
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_ascii_lowercase();
        if !anchor_tokens.contains(&token) {
            continue;
        }
        executor_addresses.insert(from);
        let amount = parse_hex_u256(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        *profit_by_token.entry(token).or_default() += amount;
    }

    if executor_addresses.is_empty() {
        return Vec::new();
    }

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
        if topic0 != ERC20_TRANSFER_TOPIC {
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
        if !executor_addresses.contains(&from) || to == collector_address {
            continue;
        }
        let token = log
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_ascii_lowercase();
        if !anchor_tokens.contains(&token) {
            continue;
        }
        let amount = parse_hex_u256(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
        *input_by_token.entry(token).or_default() += amount;
    }

    let tokens = input_by_token
        .keys()
        .chain(profit_by_token.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut out = Vec::new();
    for token in tokens {
        let input = input_by_token.get(&token).copied().unwrap_or_default();
        let profit = profit_by_token.get(&token).copied().unwrap_or_default();
        let config = anchor_configs.get(&token).cloned().unwrap_or_default();
        let configured_amounts = config
            .multihop_amounts
            .as_deref()
            .or(config.all_amounts.as_deref())
            .unwrap_or("-");
        let configured_min_profit = config
            .multihop_min_profit
            .as_deref()
            .or(config.all_min_profit.as_deref())
            .unwrap_or("-");
        out.push(format!(
            "token={} input_raw={} profit_raw={} configured_multihop_amounts={} configured_multihop_min_profit={}",
            short_addr(&token),
            input,
            profit,
            configured_amounts,
            configured_min_profit
        ));
    }
    out
}

fn write_tx(
    writer: &mut BufWriter<File>,
    tx: &CollectorTx,
    receipt: &TxReceipt,
    profits: &[String],
    coverages: &[PoolCoverage],
    diagnostics: &TxDiagnostics,
    classification: &str,
) -> Result<()> {
    writeln!(writer, "== Tx ==")?;
    writeln!(writer, "block: {}", tx.block_number)?;
    writeln!(writer, "tx: {:#x}", tx.tx_hash)?;
    writeln!(writer, "success: {}", receipt.success)?;
    writeln!(
        writer,
        "gas_used: {}",
        receipt
            .gas_used
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        writer,
        "effective_gas_price: {}",
        receipt
            .effective_gas_price
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(writer, "collector_in: {}", profits.join(", "))?;
    writeln!(writer, "classification: {classification}")?;
    writeln!(writer, "topic_summary: {}", diagnostics.topic_summary)?;
    writeln!(
        writer,
        "recognized_swap_pools: {}",
        diagnostics.recognized_swap_pools
    )?;
    writeln!(
        writer,
        "anchors_touched: {}",
        if diagnostics.anchors_touched.is_empty() {
            "-".into()
        } else {
            diagnostics
                .anchors_touched
                .iter()
                .map(|token| short_addr(token))
                .collect::<Vec<_>>()
                .join(", ")
        }
    )?;
    writeln!(
        writer,
        "recognized_anchor_cycle: {}",
        diagnostics.recognized_anchor_cycle
    )?;
    writeln!(writer, "searcher_gap: {}", diagnostics.searcher_gap)?;
    writeln!(
        writer,
        "anchor_cycles: {}",
        if diagnostics.anchor_cycles.is_empty() {
            "-".into()
        } else {
            diagnostics.anchor_cycles.join(" || ")
        }
    )?;
    writeln!(
        writer,
        "anchor_input_guess: {}",
        if diagnostics.anchor_input_guesses.is_empty() {
            "-".into()
        } else {
            diagnostics.anchor_input_guesses.join("; ")
        }
    )?;
    writeln!(
        writer,
        "unrecognized_transfer_counterparties: {}",
        if diagnostics.unrecognized_transfer_counterparties.is_empty() {
            "-".into()
        } else {
            diagnostics
                .unrecognized_transfer_counterparties
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        }
    )?;
    writeln!(writer, "transfer_flow:")?;
    for transfer in &diagnostics.transfer_flow {
        writeln!(writer, "  - {transfer}")?;
    }
    writeln!(writer, "pools:")?;
    for row in coverages {
        writeln!(
            writer,
            "  - pool={} topic={} tokens={}/{} symbol={} dex={} variant={} enabled={} state_block={} state_source={} observed={} discovery={} dex_events_same_block={} opportunities_near_block={}",
            short_addr(&row.pool),
            topic_family(&row.topic0),
            row.token0
                .as_deref()
                .map(short_addr)
                .unwrap_or_else(|| "-".into()),
            row.token1
                .as_deref()
                .map(short_addr)
                .unwrap_or_else(|| "-".into()),
            row.symbol.as_deref().unwrap_or("-"),
            row.dex.as_deref().unwrap_or("-"),
            row.variant.as_deref().unwrap_or("-"),
            row.pool_enabled
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            row.latest_state_block
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            row.latest_state_source.as_deref().unwrap_or("-"),
            row.observed_status.as_deref().unwrap_or("-"),
            row.discovery_source.as_deref().unwrap_or("-"),
            row.dex_event_logs_same_block,
            row.opportunities_near_block,
        )?;
    }
    writeln!(writer)?;
    Ok(())
}

fn is_swap_topic(topic0: &str) -> bool {
    matches!(
        topic0,
        UNI_V3_SWAP_TOPIC
            | PANCAKE_V3_SWAP_TOPIC
            | CLASSIC_SWAP_TOPIC
            | AERODROME_CLASSIC_SWAP_TOPIC
    )
}

fn topic_family(topic0: &str) -> &'static str {
    match topic0 {
        UNI_V3_SWAP_TOPIC => "uni_v3",
        PANCAKE_V3_SWAP_TOPIC => "pancake_v3",
        CLASSIC_SWAP_TOPIC => "classic",
        AERODROME_CLASSIC_SWAP_TOPIC => "aero_classic",
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

fn raw_log_tx_hash(log: &Value) -> Option<B256> {
    log.get("transactionHash")?.as_str()?.parse().ok()
}

fn raw_log_block_number(log: &Value) -> Option<u64> {
    parse_hex_u64(log.get("blockNumber")?.as_str()?)
}

fn address_topic(address: Address) -> String {
    format!("0x{:0>64}", hex::encode(address.as_slice())).to_ascii_lowercase()
}

fn parse_hex_u64(raw: &str) -> Option<u64> {
    u64::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

fn topic_address(topic: &str) -> Option<String> {
    let raw = topic.trim_start_matches("0x");
    if raw.len() < 40 {
        return None;
    }
    Some(format!("0x{}", &raw[raw.len() - 40..]).to_ascii_lowercase())
}

fn hex_to_decimal(raw: &str) -> String {
    parse_hex_u256(raw).to_string()
}

fn parse_hex_u256(raw: &str) -> U256 {
    let raw = raw.trim_start_matches("0x");
    if raw.is_empty() {
        return U256::ZERO;
    }
    U256::from_str_radix(raw, 16).unwrap_or(U256::ZERO)
}

fn short_addr(address: &str) -> String {
    let lower = address.to_ascii_lowercase();
    if lower.len() <= 12 {
        return lower;
    }
    format!("{}..{}", &lower[..8], &lower[lower.len() - 6..])
}

fn short_hash(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    if lower.len() <= 18 {
        return lower;
    }
    format!("{}..{}", &lower[..10], &lower[lower.len() - 6..])
}
