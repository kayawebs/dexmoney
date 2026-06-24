use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
};

use alloy_primitives::{Address, B256};
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
const UNISWAP_V4_SWAP_TOPIC: &str =
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
const BALANCER_V3_SWAP_TOPIC: &str =
    "0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug)]
struct Cli {
    collector: Address,
    lookback_blocks: u64,
    limit: usize,
    output: Option<PathBuf>,
    include_opportunity_lookup: bool,
}

#[derive(Debug, Clone)]
struct CollectorTx {
    tx_hash: B256,
    block_number: u64,
}

#[derive(Debug, Clone)]
struct PoolHit {
    pool: String,
    topic0: String,
}

#[derive(Debug, Clone)]
struct PoolCoverage {
    pool: String,
    token0: Option<String>,
    token1: Option<String>,
    symbol: Option<String>,
    dex: Option<String>,
    variant: Option<String>,
    enabled: Option<bool>,
    pool_source: Option<String>,
    factory_address: Option<String>,
    protocol: Option<String>,
    protocol_status: Option<String>,
    discovery_source: Option<String>,
    hooks_address: Option<String>,
    latest_state_block: Option<i64>,
    latest_state_source: Option<String>,
    tick_count: i64,
    latest_tick_block: Option<i64>,
    tick_coverage_status: Option<String>,
    tick_coverage_source: Option<String>,
    tick_coverage_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    first_seen_block: Option<i64>,
    latest_seen_block: Option<i64>,
    logs_30d: Option<i64>,
    opportunities_near_block: i64,
    balancer_runtime_ready: bool,
}

#[derive(Debug, Default)]
struct PoolAggregate {
    pool: String,
    topic0: String,
    txs: usize,
    latest_block: u64,
    coverage: Option<PoolCoverage>,
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
    let latest = provider.get_block_number().await?;
    let from_block = latest.saturating_sub(cli.lookback_blocks);

    let txs = load_collector_txs(&provider, cli.collector, from_block, latest, cli.limit).await?;
    let mut writer = report_writer(cli.output.as_ref())?;
    writeln!(writer, "== Competitor Pool Gap Report ==")?;
    writeln!(writer, "collector\t{:#x}", cli.collector)?;
    writeln!(writer, "from_block\t{from_block}")?;
    writeln!(writer, "to_block\t{latest}")?;
    writeln!(writer, "collector_transfer_txs\t{}", txs.len())?;
    writeln!(
        writer,
        "include_opportunity_lookup\t{}",
        cli.include_opportunity_lookup
    )?;
    writeln!(writer)?;

    let mut tx_gap_counts = BTreeMap::<String, usize>::new();
    let mut pool_gap_counts = BTreeMap::<String, usize>::new();
    let mut topic_counts = BTreeMap::<String, usize>::new();
    let mut pools = BTreeMap::<String, PoolAggregate>::new();
    let mut tx_rows = Vec::new();
    let balancer_runtime_ready = settings.balancer_v3_vault.is_some()
        && settings.balancer_v3_router.is_some()
        && settings.balancer_v3_adapter.is_some();

    for tx in &txs {
        let Some(receipt) = provider.get_transaction_receipt(tx.tx_hash).await? else {
            *tx_gap_counts.entry("receipt_missing".into()).or_default() += 1;
            continue;
        };
        if !receipt.success {
            *tx_gap_counts.entry("receipt_reverted".into()).or_default() += 1;
            continue;
        }
        let hits = dedupe_pool_hits(receipt_pool_hits(&receipt));
        if hits.is_empty() {
            *tx_gap_counts
                .entry("no_supported_swap_logs".into())
                .or_default() += 1;
            tx_rows.push(format!(
                "{:#x}\t{}\tno_supported_swap_logs\t-",
                tx.tx_hash, tx.block_number
            ));
            continue;
        }
        let mut coverages = Vec::with_capacity(hits.len());
        for hit in hits {
            *topic_counts
                .entry(topic_family(&hit.topic0).to_string())
                .or_default() += 1;
            let coverage = load_pool_coverage(
                &store.pool,
                settings.chain_id,
                tx.block_number,
                &hit.pool,
                cli.include_opportunity_lookup,
                balancer_runtime_ready,
            )
            .await?;
            let gap = classify_pool_gap(&coverage, cli.include_opportunity_lookup);
            *pool_gap_counts.entry(gap).or_default() += 1;
            let entry = pools
                .entry(hit.pool.clone())
                .or_insert_with(|| PoolAggregate {
                    pool: hit.pool.clone(),
                    topic0: hit.topic0.clone(),
                    txs: 0,
                    latest_block: tx.block_number,
                    coverage: None,
                });
            entry.txs += 1;
            entry.latest_block = entry.latest_block.max(tx.block_number);
            entry.coverage = Some(coverage.clone());
            coverages.push(coverage);
        }
        let tx_gap = classify_tx_gap(&coverages, cli.include_opportunity_lookup);
        *tx_gap_counts.entry(tx_gap.clone()).or_default() += 1;
        tx_rows.push(format!(
            "{:#x}\t{}\t{}\t{}",
            tx.tx_hash,
            tx.block_number,
            tx_gap,
            coverages
                .iter()
                .map(|row| format!(
                    "{}:{}",
                    short_addr(&row.pool),
                    classify_pool_gap(row, cli.include_opportunity_lookup)
                ))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }

    write_counts(&mut writer, "tx_gap_counts", &tx_gap_counts)?;
    write_counts(&mut writer, "pool_gap_counts", &pool_gap_counts)?;
    write_counts(&mut writer, "swap_topic_counts", &topic_counts)?;
    write_pool_table(&mut writer, pools)?;
    write_tx_rows(&mut writer, tx_rows)?;
    writer.flush()?;

    if let Some(path) = cli.output {
        println!("pool gap report written to {}", path.display());
    }
    Ok(())
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut collector = None;
    let mut lookback_blocks = 5_000_u64;
    let mut limit = 200_usize;
    let mut output = None;
    let mut include_opportunity_lookup = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--address" | "--collector" => {
                collector = Some(
                    Address::from_str(&iter.next().context("--address requires a value")?)
                        .context("invalid --address")?,
                );
            }
            "--lookback-blocks" => {
                lookback_blocks = iter
                    .next()
                    .context("--lookback-blocks requires a value")?
                    .parse()
                    .context("invalid --lookback-blocks")?;
            }
            "--limit" => {
                limit = iter
                    .next()
                    .context("--limit requires a value")?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--output" | "--out" => {
                output = Some(PathBuf::from(
                    iter.next().context("--output requires a value")?,
                ));
            }
            "--include-opportunity-lookup" => {
                include_opportunity_lookup = true;
            }
            "--help" | "-h" => {
                print_usage();
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
        include_opportunity_lookup,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_pool_gap -- --address <collector> [--lookback-blocks 5000] [--limit 200] [--output report.txt] [--include-opportunity-lookup]"
    );
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

fn receipt_pool_hits(receipt: &TxReceipt) -> Vec<PoolHit> {
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
            } else if topic0 == BALANCER_V3_SWAP_TOPIC {
                log.get("topics")?
                    .as_array()?
                    .get(1)?
                    .as_str()
                    .and_then(topic_address)?
            } else {
                log.get("address")?.as_str()?.to_ascii_lowercase()
            };
            Some(PoolHit { pool, topic0 })
        })
        .collect()
}

fn dedupe_pool_hits(hits: Vec<PoolHit>) -> Vec<PoolHit> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for hit in hits {
        if seen.insert(hit.pool.clone()) {
            out.push(hit);
        }
    }
    out
}

async fn load_pool_coverage(
    pool: &PgPool,
    chain_id: u64,
    block_number: u64,
    pool_address: &str,
    include_opportunity_lookup: bool,
    balancer_runtime_ready: bool,
) -> Result<PoolCoverage> {
    let opportunities_expr = if include_opportunity_lookup {
        r#"
            (
                SELECT count(*)
                FROM opportunities o, target t
                WHERE o.block_number BETWEEN GREATEST($2 - 1, 0) AND $2 + 1
                  AND lower(o.path_json::text) LIKE '%' || t.pool || '%'
            )
        "#
    } else {
        "0::bigint"
    };
    let sql = format!(
        r#"
        WITH target AS (SELECT lower($1::text) AS pool)
        SELECT
            COALESCE(p.token0, op.token0, po.token0) AS token0,
            COALESCE(p.token1, op.token1, po.token1) AS token1,
            COALESCE(tp.symbol, op.symbol, po.symbol) AS symbol,
            COALESCE(p.dex, op.dex, po.dex) AS dex,
            COALESCE(p.variant, op.variant, po.variant) AS variant,
            p.enabled AS enabled,
            p.source AS pool_source,
            COALESCE(p.factory_address, op.factory_address, po.factory_address) AS factory_address,
            po.protocol AS protocol,
            COALESCE(op.import_status, po.import_status) AS protocol_status,
            COALESCE(op.discovery_source, po.discovery_source) AS discovery_source,
            po.hooks_address AS hooks_address,
            ps.block_number AS latest_state_block,
            ps.source AS latest_state_source,
            COALESCE(pt.tick_count, 0)::bigint AS tick_count,
            pt.latest_tick_block AS latest_tick_block,
            tc.status AS tick_coverage_status,
            tc.source AS tick_coverage_source,
            tc.updated_at AS tick_coverage_updated_at,
            COALESCE(op.first_block, po.first_block) AS first_seen_block,
            COALESCE(op.latest_block, po.latest_block) AS latest_seen_block,
            COALESCE(op.logs_30d, po.logs_30d) AS logs_30d,
            {opportunities_expr} AS opportunities_near_block
        FROM target t
        LEFT JOIN pools p ON p.chain_id = $3 AND p.pool_address = t.pool
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN observed_pools op ON op.chain_id = $3 AND op.pool_address = t.pool
        LEFT JOIN LATERAL (
            SELECT
                protocol, token0, token1, symbol, factory_address, dex, variant,
                hooks_address, first_block, latest_block, logs_30d,
                discovery_source, import_status
            FROM protocol_pool_observations po
            WHERE po.chain_id = $3
              AND po.pool_address = t.pool
            ORDER BY po.updated_at DESC
            LIMIT 1
        ) po ON true
        LEFT JOIN LATERAL (
            SELECT block_number, source
            FROM pool_states ps
            WHERE ps.pool_address = t.pool
            ORDER BY block_number DESC
            LIMIT 1
        ) ps ON true
        LEFT JOIN LATERAL (
            SELECT count(*) AS tick_count, max(block_number) AS latest_tick_block
            FROM pool_ticks_current pt
            WHERE pt.chain_id = $3
              AND pt.pool_address = t.pool
        ) pt ON true
        LEFT JOIN pool_tick_coverage tc
          ON tc.chain_id = $3
         AND lower(tc.pool_address) = t.pool
        "#,
    );
    let row = sqlx::query(&sql)
        .bind(pool_address)
        .bind(i64::try_from(block_number)?)
        .bind(i64::try_from(chain_id)?)
        .fetch_one(pool)
        .await?;

    Ok(PoolCoverage {
        pool: pool_address.to_string(),
        token0: row.try_get("token0")?,
        token1: row.try_get("token1")?,
        symbol: row.try_get("symbol")?,
        dex: row.try_get("dex")?,
        variant: row.try_get("variant")?,
        enabled: row.try_get("enabled")?,
        pool_source: row.try_get("pool_source")?,
        factory_address: row.try_get("factory_address")?,
        protocol: row.try_get("protocol")?,
        protocol_status: row.try_get("protocol_status")?,
        discovery_source: row.try_get("discovery_source")?,
        hooks_address: row.try_get("hooks_address")?,
        latest_state_block: row.try_get("latest_state_block")?,
        latest_state_source: row.try_get("latest_state_source")?,
        tick_count: row.try_get("tick_count")?,
        latest_tick_block: row.try_get("latest_tick_block")?,
        tick_coverage_status: row.try_get("tick_coverage_status")?,
        tick_coverage_source: row.try_get("tick_coverage_source")?,
        tick_coverage_updated_at: row.try_get("tick_coverage_updated_at")?,
        first_seen_block: row.try_get("first_seen_block")?,
        latest_seen_block: row.try_get("latest_seen_block")?,
        logs_30d: row.try_get("logs_30d")?,
        opportunities_near_block: row.try_get("opportunities_near_block")?,
        balancer_runtime_ready,
    })
}

fn classify_pool_gap(row: &PoolCoverage, include_opportunity_lookup: bool) -> String {
    if row.symbol.is_none() && row.dex.is_none() && row.variant.is_none() {
        return "missing_metadata".into();
    }
    if row.enabled.is_none() {
        return match row.protocol_status.as_deref() {
            Some("observed_only") => "observed_only_not_imported".into(),
            Some("classified_observed_only") => "classified_observed_only_not_imported".into(),
            Some(status) => format!("not_in_pools_status_{status}"),
            None => "not_in_pools".into(),
        };
    }
    if row.enabled != Some(true) {
        return "pool_disabled".into();
    }
    if row.latest_state_block.is_none() {
        return "missing_pool_state".into();
    }
    if is_v3_style(row) && row.tick_count == 0 {
        return match row.tick_coverage_status.as_deref() {
            Some("zero_ticks") => "tick_scan_zero".into(),
            Some("refresh_failed") => "tick_scan_failed".into(),
            Some(status) => format!("tick_coverage_{status}"),
            None => "missing_ticks_unscanned".into(),
        };
    }
    if row.variant.as_deref() == Some("UniswapV4")
        && row
            .hooks_address
            .as_deref()
            .is_some_and(|hooks| hooks.to_ascii_lowercase() != ZERO_ADDRESS)
    {
        return "v4_hook_pool_currently_unsupported".into();
    }
    if row.variant.as_deref() == Some("BalancerV3") && !row.balancer_runtime_ready {
        return "balancer_v3_needs_runtime_quote_validation".into();
    }
    if include_opportunity_lookup && row.opportunities_near_block == 0 {
        return "covered_no_opportunity_near_block".into();
    }
    "ready".into()
}

fn classify_tx_gap(coverages: &[PoolCoverage], include_opportunity_lookup: bool) -> String {
    if coverages.is_empty() {
        return "no_supported_swap_logs".into();
    }
    let mut worst = "ready".to_string();
    for coverage in coverages {
        let gap = classify_pool_gap(coverage, include_opportunity_lookup);
        if gap != "ready" {
            worst = gap;
            break;
        }
    }
    worst
}

fn is_v3_style(row: &PoolCoverage) -> bool {
    matches!(
        row.variant.as_deref(),
        Some("AerodromeSlipstream" | "UniswapV3" | "PancakeV3" | "UniswapV4")
    )
}

fn write_counts(
    writer: &mut BufWriter<Box<dyn Write>>,
    title: &str,
    counts: &BTreeMap<String, usize>,
) -> Result<()> {
    writeln!(writer, "== {title} ==")?;
    for (key, value) in counts {
        writeln!(writer, "{key}\t{value}")?;
    }
    writeln!(writer)?;
    Ok(())
}

fn write_pool_table(
    writer: &mut BufWriter<Box<dyn Write>>,
    pools: BTreeMap<String, PoolAggregate>,
) -> Result<()> {
    writeln!(writer, "== Unique Competitor Pools ==")?;
    writeln!(
        writer,
        "pool\ttxs\tlatest_block\ttopic\tgap\tsymbol\tdex\tvariant\ttoken0\ttoken1\tenabled\tpool_source\tstate_block\tstate_source\tticks\tlatest_tick_block\ttick_coverage_status\ttick_coverage_source\ttick_coverage_updated_at\tprotocol\tprotocol_status\tdiscovery\tfirst_seen\tlatest_seen\tlogs_30d\thooks\tfactory"
    )?;
    let mut rows = pools.into_values().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .txs
            .cmp(&left.txs)
            .then_with(|| right.latest_block.cmp(&left.latest_block))
            .then_with(|| left.pool.cmp(&right.pool))
    });
    for row in rows {
        let coverage = row.coverage.as_ref();
        let gap = coverage
            .map(|coverage| classify_pool_gap(coverage, false))
            .unwrap_or_else(|| "missing_coverage".into());
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.pool,
            row.txs,
            row.latest_block,
            topic_family(&row.topic0),
            gap,
            opt(coverage.and_then(|c| c.symbol.as_deref())),
            opt(coverage.and_then(|c| c.dex.as_deref())),
            opt(coverage.and_then(|c| c.variant.as_deref())),
            opt(coverage.and_then(|c| c.token0.as_deref())),
            opt(coverage.and_then(|c| c.token1.as_deref())),
            coverage
                .and_then(|c| c.enabled)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            opt(coverage.and_then(|c| c.pool_source.as_deref())),
            coverage
                .and_then(|c| c.latest_state_block)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            opt(coverage.and_then(|c| c.latest_state_source.as_deref())),
            coverage
                .map(|c| c.tick_count.to_string())
                .unwrap_or_else(|| "-".into()),
            coverage
                .and_then(|c| c.latest_tick_block)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            opt(coverage.and_then(|c| c.tick_coverage_status.as_deref())),
            opt(coverage.and_then(|c| c.tick_coverage_source.as_deref())),
            coverage
                .and_then(|c| c.tick_coverage_updated_at)
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "-".into()),
            opt(coverage.and_then(|c| c.protocol.as_deref())),
            opt(coverage.and_then(|c| c.protocol_status.as_deref())),
            opt(coverage.and_then(|c| c.discovery_source.as_deref())),
            coverage
                .and_then(|c| c.first_seen_block)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            coverage
                .and_then(|c| c.latest_seen_block)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            coverage
                .and_then(|c| c.logs_30d)
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            opt(coverage.and_then(|c| c.hooks_address.as_deref())),
            opt(coverage.and_then(|c| c.factory_address.as_deref())),
        )?;
    }
    writeln!(writer)?;
    Ok(())
}

fn write_tx_rows(writer: &mut BufWriter<Box<dyn Write>>, rows: Vec<String>) -> Result<()> {
    writeln!(writer, "== Sample Tx Gaps ==")?;
    writeln!(writer, "tx_hash\tblock\tgap\tpools")?;
    for row in rows {
        writeln!(writer, "{row}")?;
    }
    Ok(())
}

fn report_writer(path: Option<&PathBuf>) -> Result<BufWriter<Box<dyn Write>>> {
    if let Some(path) = path {
        Ok(BufWriter::new(Box::new(File::create(path)?)))
    } else {
        Ok(BufWriter::new(Box::new(std::io::stdout())))
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
            | BALANCER_V3_SWAP_TOPIC
    )
}

fn topic_family(topic0: &str) -> &'static str {
    match topic0 {
        UNI_V3_SWAP_TOPIC => "uni_v3",
        PANCAKE_V3_SWAP_TOPIC => "pancake_v3",
        CLASSIC_SWAP_TOPIC => "classic",
        AERODROME_CLASSIC_SWAP_TOPIC => "aero_classic",
        UNISWAP_V4_SWAP_TOPIC => "uniswap_v4",
        BALANCER_V3_SWAP_TOPIC => "balancer_v3",
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
    log.get("transactionHash")?
        .as_str()
        .and_then(|raw| B256::from_str(raw).ok())
}

fn raw_log_block_number(log: &Value) -> Option<u64> {
    let raw = log.get("blockNumber")?.as_str()?.trim_start_matches("0x");
    u64::from_str_radix(raw, 16).ok()
}

fn address_topic(address: Address) -> String {
    format!(
        "0x{:0>64}",
        format!("{address:#x}").trim_start_matches("0x")
    )
}

fn topic_address(topic: &str) -> Option<String> {
    let raw = topic.trim_start_matches("0x");
    if raw.len() != 64 {
        return None;
    }
    Some(format!("0x{}", &raw[24..]).to_ascii_lowercase())
}

fn synthetic_address_from_pool_uid(pool_uid: &str) -> Option<String> {
    topic_address(pool_uid)
}

fn short_addr(address: &str) -> String {
    if address.len() <= 12 {
        return address.to_string();
    }
    format!("{}..{}", &address[..6], &address[address.len() - 4..])
}

fn opt(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}
