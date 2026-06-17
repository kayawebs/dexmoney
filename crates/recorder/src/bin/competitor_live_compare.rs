use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
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
    writeln!(writer)?;

    let mut summary = BTreeMap::<String, usize>::new();
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
        *summary.entry(classification.clone()).or_default() += 1;
        write_tx(
            &mut writer,
            &tx,
            &receipt,
            &profits,
            &coverages,
            &classification,
        )?;
    }

    writeln!(writer)?;
    writeln!(writer, "== Summary ==")?;
    for (classification, n) in summary {
        writeln!(writer, "{classification}\t{n}")?;
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

fn write_tx(
    writer: &mut BufWriter<File>,
    tx: &CollectorTx,
    receipt: &TxReceipt,
    profits: &[String],
    coverages: &[PoolCoverage],
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
    writeln!(writer, "pools:")?;
    for row in coverages {
        writeln!(
            writer,
            "  - pool={} topic={} symbol={} dex={} variant={} enabled={} state_block={} state_source={} observed={} discovery={} dex_events_same_block={} opportunities_near_block={}",
            short_addr(&row.pool),
            topic_family(&row.topic0),
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
        UNI_V3_SWAP_TOPIC | PANCAKE_V3_SWAP_TOPIC | CLASSIC_SWAP_TOPIC
    )
}

fn topic_family(topic0: &str) -> &'static str {
    match topic0 {
        UNI_V3_SWAP_TOPIC => "uni_v3",
        PANCAKE_V3_SWAP_TOPIC => "pancake_v3",
        CLASSIC_SWAP_TOPIC => "classic",
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

fn hex_to_decimal(raw: &str) -> String {
    alloy_primitives::U256::from_str_radix(raw.trim_start_matches("0x"), 16)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| "0".into())
}

fn short_addr(address: &str) -> String {
    let lower = address.to_ascii_lowercase();
    if lower.len() <= 12 {
        return lower;
    }
    format!("{}..{}", &lower[..8], &lower[lower.len() - 6..])
}
