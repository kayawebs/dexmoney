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
    top: usize,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CollectorTx {
    tx_hash: B256,
    block_number: u64,
}

#[derive(Debug, Clone)]
struct PoolCoverage {
    pool: String,
}

#[derive(Debug, Clone, Default)]
struct CounterpartyStats {
    txs: BTreeSet<B256>,
    transfer_in: u64,
    transfer_out: u64,
    emitted_topics: BTreeMap<String, u64>,
    sample_tokens: BTreeSet<String>,
    sample_neighbors: BTreeSet<String>,
}

#[derive(Debug, Clone, Default)]
struct DbProfile {
    pool_symbol: Option<String>,
    pool_dex: Option<String>,
    pool_variant: Option<String>,
    pool_enabled: Option<bool>,
    observed_status: Option<String>,
    observed_reason: Option<String>,
    token_symbol: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct RpcProfile {
    code_bytes: usize,
    token0: Option<String>,
    token1: Option<String>,
    factory: Option<String>,
    slot0: bool,
    liquidity: Option<U256>,
    reserves: bool,
    stable: Option<bool>,
    decimals: Option<u8>,
    total_supply: Option<U256>,
    symbol: Option<String>,
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
    .await?;

    let out_path = cli.output.unwrap_or_else(|| {
        PathBuf::from(format!(
            "reports/competitor-flow-probe-{}-{}.txt",
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

    writeln!(writer, "competitor flow probe")?;
    writeln!(writer, "collector: {:#x}", cli.collector)?;
    writeln!(writer, "from_block: {from_block}")?;
    writeln!(writer, "to_block: {latest_block}")?;
    writeln!(writer, "collector_transfer_txs: {}", txs.len())?;
    writeln!(writer)?;

    let mut stats = BTreeMap::<String, CounterpartyStats>::new();
    let mut tx_rows = Vec::new();
    for tx in txs {
        let Some(receipt) = provider.get_transaction_receipt(tx.tx_hash).await? else {
            continue;
        };
        let pools = load_coverages_for_receipt(&receipt);
        let unknown = unrecognized_transfer_counterparties(&receipt, cli.collector, &pools);
        let collector_in = collector_inbound_transfers(&receipt, cli.collector).join(", ");
        tx_rows.push((
            tx.block_number,
            tx.tx_hash,
            collector_in,
            pools.len(),
            unknown.clone(),
        ));
        record_counterparty_stats(&mut stats, &receipt, &unknown);
    }

    writeln!(writer, "== Tx Gap Samples ==")?;
    for (block, tx_hash, collector_in, recognized_pools, unknown) in &tx_rows {
        writeln!(
            writer,
            "block={} tx={:#x} collector_in={} recognized_pools={} unknown={}",
            block,
            tx_hash,
            if collector_in.is_empty() {
                "-"
            } else {
                collector_in
            },
            recognized_pools,
            if unknown.is_empty() {
                "-".into()
            } else {
                format_set(unknown)
            }
        )?;
    }
    writeln!(writer)?;

    let mut ranked = stats.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_address, left), (right_address, right)| {
        right
            .txs
            .len()
            .cmp(&left.txs.len())
            .then_with(|| right.transfer_in.cmp(&left.transfer_in))
            .then_with(|| right.transfer_out.cmp(&left.transfer_out))
            .then_with(|| left_address.cmp(right_address))
    });
    ranked.truncate(cli.top);

    writeln!(writer, "== Counterparty Probe ==")?;
    let mut importable = Vec::new();
    for (address, stats) in ranked {
        let address = Address::from_str(&address)
            .with_context(|| format!("invalid counterparty address {address}"))?;
        let db = load_db_profile(&store.pool, settings.chain_id, address).await?;
        let rpc = probe_rpc_profile(&provider, address).await;
        let classification = classify_counterparty(&db, rpc.as_ref().ok());
        let action = recommended_action(classification, &db, rpc.as_ref().ok());
        writeln!(writer, "address: {:#x}", address)?;
        writeln!(writer, "classification: {classification}")?;
        writeln!(writer, "recommended_action: {action}")?;
        writeln!(
            writer,
            "usage: txs={} transfer_in={} transfer_out={}",
            stats.txs.len(),
            stats.transfer_in,
            stats.transfer_out
        )?;
        writeln!(
            writer,
            "emitted_topics: {}",
            format_topic_counts(&stats.emitted_topics)
        )?;
        writeln!(
            writer,
            "sample_tokens: {}",
            format_set(&stats.sample_tokens)
        )?;
        writeln!(
            writer,
            "sample_neighbors: {}",
            format_set(&stats.sample_neighbors)
        )?;
        writeln!(
            writer,
            "db: pool_symbol={} dex={} variant={} pool_enabled={} observed_status={} observed_reason={} token_symbol={}",
            db.pool_symbol.as_deref().unwrap_or("-"),
            db.pool_dex.as_deref().unwrap_or("-"),
            db.pool_variant.as_deref().unwrap_or("-"),
            db.pool_enabled
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            db.observed_status.as_deref().unwrap_or("-"),
            db.observed_reason.as_deref().unwrap_or("-"),
            db.token_symbol.as_deref().unwrap_or("-"),
        )?;
        match rpc {
            Ok(rpc) => {
                if classification == "pool_like_unregistered" {
                    importable.push(importable_pool_line(address, &rpc));
                }
                writeln!(
                    writer,
                    "rpc: code_bytes={} token0={} token1={} factory={} slot0={} liquidity={} reserves={} stable={} decimals={} total_supply={} symbol={}",
                    rpc.code_bytes,
                    rpc.token0.as_deref().unwrap_or("-"),
                    rpc.token1.as_deref().unwrap_or("-"),
                    rpc.factory.as_deref().unwrap_or("-"),
                    rpc.slot0,
                    rpc.liquidity
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    rpc.reserves,
                    rpc.stable
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    rpc.decimals
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    rpc.total_supply
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".into()),
                    rpc.symbol.as_deref().unwrap_or("-"),
                )?;
            }
            Err(err) => {
                writeln!(writer, "rpc_error: {err:#}")?;
            }
        }
        writeln!(writer)?;
    }
    if !importable.is_empty() {
        writeln!(writer, "== Importable Pool-like Counterparties ==")?;
        for line in importable {
            writeln!(writer, "{line}")?;
        }
        writeln!(writer)?;
    }
    writer.flush()?;

    println!("wrote {}", out_path.display());
    Ok(())
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut collector = None;
    let mut lookback_blocks = 500u64;
    let mut limit = 50usize;
    let mut top = 30usize;
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
            "--top" => {
                top = args
                    .next()
                    .context("--top requires a value")?
                    .parse()
                    .context("invalid --top")?;
            }
            "--output" | "--out" => {
                output = Some(PathBuf::from(
                    args.next().context("--output requires a value")?,
                ));
            }
            "--help" | "-h" => {
                println!("Usage: cargo run -p base-arb-recorder --bin competitor_flow_probe -- --address <collector> [--lookback-blocks 500] [--limit 50] [--top 30] [--output report.txt]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(Cli {
        collector: collector.context("--address is required")?,
        lookback_blocks,
        limit,
        top,
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

fn load_coverages_for_receipt(receipt: &TxReceipt) -> Vec<PoolCoverage> {
    dedupe_swap_pools(receipt)
        .into_iter()
        .map(|pool| PoolCoverage { pool })
        .collect()
}

fn dedupe_swap_pools(receipt: &TxReceipt) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
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
        if !is_swap_topic(&topic0) {
            continue;
        }
        let Some(pool) = log.get("address").and_then(Value::as_str) else {
            continue;
        };
        let pool = pool.to_ascii_lowercase();
        if seen.insert(pool.clone()) {
            out.push(pool);
        }
    }
    out
}

fn unrecognized_transfer_counterparties(
    receipt: &TxReceipt,
    collector: Address,
    coverages: &[PoolCoverage],
) -> BTreeSet<String> {
    let collector_address = format!("{:#x}", collector).to_ascii_lowercase();
    let mut known = coverages
        .iter()
        .map(|coverage| coverage.pool.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    known.insert(collector_address.clone());
    known.insert("0x0000000000000000000000000000000000000000".into());

    for log in erc20_transfer_logs(receipt) {
        let Some((from, to, _, _)) = parse_transfer_log(log) else {
            continue;
        };
        if to == collector_address {
            known.insert(from);
        }
    }

    let mut out = BTreeSet::new();
    for log in erc20_transfer_logs(receipt) {
        let Some((from, to, _, _)) = parse_transfer_log(log) else {
            continue;
        };
        for address in [from, to] {
            if !known.contains(&address) {
                out.insert(address);
            }
        }
    }
    out
}

fn record_counterparty_stats(
    stats: &mut BTreeMap<String, CounterpartyStats>,
    receipt: &TxReceipt,
    unknown: &BTreeSet<String>,
) {
    for log in erc20_transfer_logs(receipt) {
        let Some((from, to, token, _amount)) = parse_transfer_log(log) else {
            continue;
        };
        for address in unknown {
            if &from == address {
                let entry = stats.entry(address.clone()).or_default();
                entry.txs.insert(receipt.tx_hash);
                entry.transfer_out += 1;
                entry.sample_tokens.insert(token.clone());
                entry.sample_neighbors.insert(to.clone());
            }
            if &to == address {
                let entry = stats.entry(address.clone()).or_default();
                entry.txs.insert(receipt.tx_hash);
                entry.transfer_in += 1;
                entry.sample_tokens.insert(token.clone());
                entry.sample_neighbors.insert(from.clone());
            }
        }
    }

    for log in receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(address) = log.get("address").and_then(Value::as_str) else {
            continue;
        };
        let address = address.to_ascii_lowercase();
        if !unknown.contains(&address) {
            continue;
        }
        let topic = raw_log_topic0(log).unwrap_or_else(|| "no_topic0".into());
        let entry = stats.entry(address).or_default();
        *entry.emitted_topics.entry(topic).or_default() += 1;
        entry.txs.insert(receipt.tx_hash);
    }
}

async fn load_db_profile(pool: &PgPool, chain_id: u64, address: Address) -> Result<DbProfile> {
    let row = sqlx::query(
        r#"
        WITH target AS (SELECT lower($1::text) AS address)
        SELECT
            tp.symbol AS pool_symbol,
            p.dex AS pool_dex,
            p.variant AS pool_variant,
            p.enabled AS pool_enabled,
            op.import_status AS observed_status,
            op.import_reason AS observed_reason,
            t.symbol AS token_symbol
        FROM target x
        LEFT JOIN pools p ON lower(p.pool_address) = x.address
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN observed_pools op ON op.chain_id = $2 AND lower(op.pool_address) = x.address
        LEFT JOIN tokens t ON t.chain_id = $2 AND lower(t.token_address) = x.address
        "#,
    )
    .bind(format!("{address:#x}"))
    .bind(i64::try_from(chain_id)?)
    .fetch_one(pool)
    .await?;
    Ok(DbProfile {
        pool_symbol: row.try_get("pool_symbol")?,
        pool_dex: row.try_get("pool_dex")?,
        pool_variant: row.try_get("pool_variant")?,
        pool_enabled: row.try_get("pool_enabled")?,
        observed_status: row.try_get("observed_status")?,
        observed_reason: row.try_get("observed_reason")?,
        token_symbol: row.try_get("token_symbol")?,
    })
}

async fn probe_rpc_profile(provider: &ChainProvider, address: Address) -> Result<RpcProfile> {
    let code = provider.get_code(address).await?;
    let code_bytes = code.trim_start_matches("0x").len() / 2;
    let token0 = call_address(provider, address, "0x0dfe1681", "token0()").await;
    let token1 = call_address(provider, address, "0xd21220a7", "token1()").await;
    let factory = call_address(provider, address, "0xc45a0155", "factory()").await;
    let slot0 = call_raw(provider, address, "0x3850c7bd", "slot0()")
        .await
        .is_some();
    let liquidity = call_u256(provider, address, "0x1a686502", "liquidity()").await;
    let reserves = call_raw(provider, address, "0x0902f1ac", "getReserves()")
        .await
        .is_some();
    let stable = call_bool(provider, address, "0x22be3de1", "stable()").await;
    let decimals = call_u256(provider, address, "0x313ce567", "decimals()")
        .await
        .and_then(|value| u8::try_from(value.to::<u64>()).ok());
    let total_supply = call_u256(provider, address, "0x18160ddd", "totalSupply()").await;
    let symbol = call_symbol(provider, address).await;
    Ok(RpcProfile {
        code_bytes,
        token0,
        token1,
        factory,
        slot0,
        liquidity,
        reserves,
        stable,
        decimals,
        total_supply,
        symbol,
    })
}

fn classify_counterparty(db: &DbProfile, rpc: Option<&RpcProfile>) -> &'static str {
    if db.pool_symbol.is_some() {
        return "known_pool";
    }
    if db.token_symbol.is_some() {
        return "known_token";
    }
    let Some(rpc) = rpc else {
        return "rpc_probe_failed";
    };
    if rpc.code_bytes == 0 {
        return "eoa";
    }
    if rpc.token0.is_some() && rpc.token1.is_some() && (rpc.slot0 || rpc.reserves) {
        return "pool_like_unregistered";
    }
    if rpc.token0.is_some() && rpc.token1.is_some() {
        return "pair_like_no_state";
    }
    if rpc.decimals.is_some() && rpc.total_supply.is_some() {
        return "erc20_like";
    }
    "contract_unknown_or_router"
}

fn recommended_action(
    classification: &str,
    db: &DbProfile,
    rpc: Option<&RpcProfile>,
) -> &'static str {
    match classification {
        "known_pool" => {
            if db.pool_enabled == Some(true) {
                "already_known_pool; if this still forms a gap, inspect swap-topic coverage or path composition"
            } else {
                "known_pool_disabled; decide whether to enable it"
            }
        }
        "known_token" | "eoa" | "erc20_like" => "not_a_pool; usually ignore for pool coverage",
        "pool_like_unregistered" => {
            let Some(rpc) = rpc else {
                return "rpc_failed; rerun probe";
            };
            if rpc.factory.is_some() {
                "pool_like_unregistered; import/classify this pool or trust its factory if executor supports it"
            } else {
                "pool_like_unregistered_without_factory; classify manually before importing"
            }
        }
        "pair_like_no_state" => "pair_like_but_no_readable_state; protocol adapter likely needed",
        "contract_unknown_or_router" => {
            "router_or_vault_gap; decode emitted topics and trace neighboring pools before treating as executable pool"
        }
        "rpc_probe_failed" => "rpc_probe_failed; rerun with a smaller window or inspect RPC errors",
        _ => "unknown; inspect manually",
    }
}

fn importable_pool_line(address: Address, rpc: &RpcProfile) -> String {
    format!(
        "pool={:#x} token0={} token1={} factory={} stable={} reserves={} slot0={} liquidity={} symbol={}",
        address,
        rpc.token0.as_deref().unwrap_or("-"),
        rpc.token1.as_deref().unwrap_or("-"),
        rpc.factory.as_deref().unwrap_or("-"),
        rpc.stable
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        rpc.reserves,
        rpc.slot0,
        rpc.liquidity
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".into()),
        rpc.symbol.as_deref().unwrap_or("-"),
    )
}

async fn call_raw(
    provider: &ChainProvider,
    address: Address,
    data: &str,
    label: &str,
) -> Option<String> {
    provider
        .eth_call_from(None, address, data, label)
        .await
        .ok()
}

async fn call_address(
    provider: &ChainProvider,
    address: Address,
    data: &str,
    label: &str,
) -> Option<String> {
    let raw = call_raw(provider, address, data, label).await?;
    decode_word_address(&raw)
}

async fn call_u256(
    provider: &ChainProvider,
    address: Address,
    data: &str,
    label: &str,
) -> Option<U256> {
    let raw = call_raw(provider, address, data, label).await?;
    decode_first_word_u256(&raw)
}

async fn call_bool(
    provider: &ChainProvider,
    address: Address,
    data: &str,
    label: &str,
) -> Option<bool> {
    call_u256(provider, address, data, label)
        .await
        .map(|value| !value.is_zero())
}

async fn call_symbol(provider: &ChainProvider, address: Address) -> Option<String> {
    let raw = call_raw(provider, address, "0x95d89b41", "symbol()").await?;
    decode_abi_string_or_bytes32(&raw)
}

fn collector_inbound_transfers(receipt: &TxReceipt, collector: Address) -> Vec<String> {
    let collector_address = format!("{:#x}", collector).to_ascii_lowercase();
    erc20_transfer_logs(receipt)
        .filter_map(|log| {
            let (from, to, token, amount) = parse_transfer_log(log)?;
            if to != collector_address {
                return None;
            }
            Some(format!("token={} from={} amount={}", token, from, amount))
        })
        .collect()
}

fn erc20_transfer_logs(receipt: &TxReceipt) -> impl Iterator<Item = &Value> {
    receipt
        .raw
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|log| raw_log_topic0(log).as_deref() == Some(ERC20_TRANSFER_TOPIC))
}

fn parse_transfer_log(log: &Value) -> Option<(String, String, String, String)> {
    let topics = log.get("topics")?.as_array()?;
    if topics.len() < 3 {
        return None;
    }
    let from = topics[1].as_str().and_then(topic_address)?;
    let to = topics[2].as_str().and_then(topic_address)?;
    let token = log.get("address")?.as_str()?.to_ascii_lowercase();
    let amount = hex_to_decimal(log.get("data").and_then(Value::as_str).unwrap_or("0x0"));
    Some((from, to, token, amount))
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

fn topic_address(topic: &str) -> Option<String> {
    let raw = topic.trim_start_matches("0x");
    if raw.len() < 40 {
        return None;
    }
    Some(format!("0x{}", &raw[raw.len() - 40..]).to_ascii_lowercase())
}

fn parse_hex_u64(raw: &str) -> Option<u64> {
    u64::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

fn hex_to_decimal(raw: &str) -> String {
    U256::from_str_radix(raw.trim_start_matches("0x"), 16)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| "0".into())
}

fn decode_word_address(raw: &str) -> Option<String> {
    let raw = raw.trim_start_matches("0x");
    if raw.len() < 64 {
        return None;
    }
    Some(format!("0x{}", &raw[24..64]).to_ascii_lowercase())
}

fn decode_first_word_u256(raw: &str) -> Option<U256> {
    let raw = raw.trim_start_matches("0x");
    if raw.len() < 64 {
        return None;
    }
    U256::from_str_radix(&raw[..64], 16).ok()
}

fn decode_abi_string_or_bytes32(raw: &str) -> Option<String> {
    let raw = raw.trim_start_matches("0x");
    if raw.len() == 64 {
        let bytes = hex::decode(raw).ok()?;
        return Some(
            String::from_utf8_lossy(&bytes)
                .trim_matches(char::from(0))
                .trim()
                .to_string(),
        )
        .filter(|value| !value.is_empty());
    }
    if raw.len() < 128 {
        return None;
    }
    let offset = usize::from_str_radix(&raw[..64], 16).ok()?;
    let len_start = offset.checked_mul(2)?;
    let len_end = len_start.checked_add(64)?;
    if raw.len() < len_end {
        return None;
    }
    let len = usize::from_str_radix(&raw[len_start..len_end], 16).ok()?;
    let data_start = len_end;
    let data_end = data_start.checked_add(len.checked_mul(2)?)?;
    if raw.len() < data_end {
        return None;
    }
    let bytes = hex::decode(&raw[data_start..data_end]).ok()?;
    Some(String::from_utf8_lossy(&bytes).to_string()).filter(|value| !value.is_empty())
}

fn format_topic_counts(topics: &BTreeMap<String, u64>) -> String {
    if topics.is_empty() {
        return "-".into();
    }
    topics
        .iter()
        .map(|(topic, n)| format!("{topic}:{n}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_set(values: &BTreeSet<String>) -> String {
    if values.is_empty() {
        return "-".into();
    }
    values
        .iter()
        .take(12)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ")
}
