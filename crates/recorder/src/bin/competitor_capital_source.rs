use std::{
    cmp::Ordering,
    collections::BTreeMap,
    env,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const V3_FLASH_TOPIC: &str = "0xbdbdb71d7860376ba52b25a5028beea23581364a40522f6bcfb86bb1f2dca633";
const WETH: &str = "0x4200000000000000000000000000000000000006";
const USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const CBBTC: &str = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
const AERO: &str = "0x940181a94a35a4569e4529a3cdfb74e38fd98631";
const VIRTUAL: &str = "0x0b3e328455c4059eeb9e3f84b5543f74e24e7e1b";
const APPROX_BASE_BLOCKS_PER_DAY: i64 = 43_200;

static REPORT_OUTPUT: OnceLock<Mutex<Option<File>>> = OnceLock::new();

macro_rules! println {
    () => {
        crate::write_report_line(format_args!(""))
    };
    ($($arg:tt)*) => {
        crate::write_report_line(format_args!($($arg)*))
    };
}

#[derive(Debug, Clone)]
struct Cli {
    address: Address,
    days: i64,
    from_block: Option<i64>,
    to_block: Option<i64>,
    limit: i64,
    output: Option<PathBuf>,
}

#[derive(Debug, FromRow)]
struct TxRow {
    tx_hash: String,
    block_number: i64,
    transaction_index: Option<i64>,
    from_address: Option<String>,
    to_address: Option<String>,
    gas_used: Option<String>,
    effective_gas_price: Option<String>,
    tx_json: Value,
    receipt_json: Value,
}

#[derive(Debug, Clone)]
struct TokenMeta {
    symbol: String,
    decimals: u32,
}

#[derive(Debug, Clone)]
struct TransferLog {
    log_index: i64,
    token: String,
    from: String,
    to: String,
    amount: U256,
}

#[derive(Debug, Clone)]
struct FlashEvidence {
    log_index: i64,
    address: String,
    topic0: String,
    label: String,
}

#[derive(Debug, Clone, Default)]
struct TokenFlow {
    profit_to_collector_raw: U256,
    executor_out_non_collector_raw: U256,
    executor_max_out_non_collector_raw: U256,
    executor_in_non_collector_raw: U256,
    first_executor_direction: Option<TransferDirection>,
    first_executor_counterparty: Option<String>,
    first_executor_log_index: Option<i64>,
    transfer_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TransferDirection {
    In,
    Out,
}

impl TransferDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CapitalClass {
    DefiniteFlashLoan,
    PrefundedOwnCapital,
    InboundFirstOrBorrowed,
    ProfitTransferOnly,
    Unclassified,
}

impl CapitalClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::DefiniteFlashLoan => "definite_flashloan",
            Self::PrefundedOwnCapital => "prefunded_own_capital",
            Self::InboundFirstOrBorrowed => "inbound_first_or_borrowed",
            Self::ProfitTransferOnly => "profit_transfer_only",
            Self::Unclassified => "unclassified",
        }
    }
}

#[derive(Debug, Clone)]
struct TxAnalysis {
    tx_hash: String,
    block_number: i64,
    transaction_index: Option<i64>,
    from_address: String,
    executor: String,
    gas_used: Option<u128>,
    effective_gas_price: Option<u128>,
    input_selector: Option<String>,
    capital_class: CapitalClass,
    flows: BTreeMap<String, TokenFlow>,
    flash_evidence: Vec<FlashEvidence>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    if let Some(path) = cli.output.as_deref() {
        set_report_output(path)?;
    }

    let settings = Settings::load()?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;

    let (from_block, to_block) = resolve_block_range(&store.pool, &cli).await?;
    let token_meta = load_token_meta(&store.pool).await?;
    let rows = load_related_transactions(&store.pool, &cli, from_block, to_block).await?;
    let collector = address_to_lower(cli.address);
    let analyses = rows
        .into_iter()
        .filter_map(|row| analyze_tx(row, &collector).transpose())
        .collect::<Result<Vec<_>>>()?;

    print_report(&cli, from_block, to_block, &token_meta, &analyses);
    Ok(())
}

fn print_report(
    cli: &Cli,
    from_block: i64,
    to_block: i64,
    token_meta: &BTreeMap<String, TokenMeta>,
    txs: &[TxAnalysis],
) {
    println!("== Scope ==");
    println!("collector: {}", address_to_lower(cli.address));
    println!("days: {}", cli.days);
    println!("blocks: {from_block}..{to_block}");
    println!("related_profit_txs: {}", txs.len());
    println!();

    print_classification_summary(txs);
    print_flash_evidence(txs);
    print_upstream_summary(txs);
    print_source_token_distribution(token_meta, txs);
    print_profit_token_distribution(token_meta, txs);
    print_first_executor_transfer(token_meta, txs);
    print_examples(token_meta, txs);
}

fn print_classification_summary(txs: &[TxAnalysis]) {
    println!("== Capital Source Classification ==");
    let mut rows: BTreeMap<CapitalClass, usize> = BTreeMap::new();
    for tx in txs {
        *rows.entry(tx.capital_class).or_default() += 1;
    }
    for (class, n) in rows {
        println!("class={} txs={}", class.as_str(), n);
    }
    println!();
}

fn print_flash_evidence(txs: &[TxAnalysis]) {
    println!("== Flash Loan / Flash Swap Evidence ==");
    let flash_txs = txs
        .iter()
        .filter(|tx| !tx.flash_evidence.is_empty())
        .collect::<Vec<_>>();
    if flash_txs.is_empty() {
        println!("no explicit Flash/FlashLoan event topics in cached receipts");
        println!();
        return;
    }
    println!("flash_evidence_txs={}", flash_txs.len());
    for tx in flash_txs.iter().take(80) {
        for evidence in &tx.flash_evidence {
            println!(
                "block={} idx={} tx={} executor={} log_index={} emitter={} label={} topic0={}",
                tx.block_number,
                tx.transaction_index
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into()),
                tx.tx_hash,
                tx.executor,
                evidence.log_index,
                evidence.address,
                evidence.label,
                evidence.topic0,
            );
        }
    }
    println!();
}

fn print_upstream_summary(txs: &[TxAnalysis]) {
    println!("== Upstream Execution Addresses ==");
    let mut by_lane: BTreeMap<(String, String), usize> = BTreeMap::new();
    for tx in txs {
        *by_lane
            .entry((tx.from_address.clone(), tx.executor.clone()))
            .or_default() += 1;
    }
    let mut rows = by_lane.into_iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    for ((from, executor), n) in rows.into_iter().take(40) {
        println!("from={from} executor={executor} txs={n}");
    }
    println!();
}

fn print_source_token_distribution(token_meta: &BTreeMap<String, TokenMeta>, txs: &[TxAnalysis]) {
    println!("== Executor Outbound Capital By Token ==");
    let mut by_token: BTreeMap<String, Vec<U256>> = BTreeMap::new();
    for tx in txs {
        if tx.capital_class == CapitalClass::DefiniteFlashLoan {
            continue;
        }
        for (token, flow) in &tx.flows {
            if flow.executor_out_non_collector_raw > U256::ZERO {
                by_token
                    .entry(token.clone())
                    .or_default()
                    .push(flow.executor_max_out_non_collector_raw);
            }
        }
    }
    let mut rows = by_token.into_iter().collect::<Vec<_>>();
    rows.sort_by(|(_, a), (_, b)| b.len().cmp(&a.len()));
    if rows.is_empty() {
        println!("no executor outbound capital transfers found");
        println!();
        return;
    }
    for (token, values) in rows.into_iter().take(40) {
        let meta = token_meta_for(token_meta, &token);
        let formatted = values
            .iter()
            .map(|value| units_to_f64(*value, meta.decimals))
            .collect::<Vec<_>>();
        println!(
            "token={} address={} txs={} capital_max_out p50={:.8} p90={:.8} p99={:.8} max={:.8}",
            meta.symbol,
            token,
            formatted.len(),
            percentile(formatted.clone(), 0.50),
            percentile(formatted.clone(), 0.90),
            percentile(formatted.clone(), 0.99),
            max_value(formatted),
        );
    }
    println!();
}

fn print_profit_token_distribution(token_meta: &BTreeMap<String, TokenMeta>, txs: &[TxAnalysis]) {
    println!("== Profit To Collector By Token ==");
    let mut by_token: BTreeMap<String, Vec<U256>> = BTreeMap::new();
    for tx in txs {
        for (token, flow) in &tx.flows {
            if flow.profit_to_collector_raw > U256::ZERO {
                by_token
                    .entry(token.clone())
                    .or_default()
                    .push(flow.profit_to_collector_raw);
            }
        }
    }
    let mut rows = by_token.into_iter().collect::<Vec<_>>();
    rows.sort_by(|(_, a), (_, b)| b.len().cmp(&a.len()));
    if rows.is_empty() {
        println!("no collector profit transfers found");
        println!();
        return;
    }
    for (token, values) in rows.into_iter().take(40) {
        let meta = token_meta_for(token_meta, &token);
        let formatted = values
            .iter()
            .map(|value| units_to_f64(*value, meta.decimals))
            .collect::<Vec<_>>();
        println!(
            "token={} address={} txs={} profit p50={:.8} p90={:.8} p99={:.8} max={:.8}",
            meta.symbol,
            token,
            formatted.len(),
            percentile(formatted.clone(), 0.50),
            percentile(formatted.clone(), 0.90),
            percentile(formatted.clone(), 0.99),
            max_value(formatted),
        );
    }
    println!();
}

fn print_first_executor_transfer(token_meta: &BTreeMap<String, TokenMeta>, txs: &[TxAnalysis]) {
    println!("== First Executor Transfer Direction ==");
    let mut rows: BTreeMap<(String, TransferDirection), usize> = BTreeMap::new();
    for tx in txs {
        for (token, flow) in &tx.flows {
            if let Some(direction) = flow.first_executor_direction {
                *rows.entry((token.clone(), direction)).or_default() += 1;
            }
        }
    }
    let mut rows = rows.into_iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    for ((token, direction), n) in rows.into_iter().take(60) {
        let meta = token_meta_for(token_meta, &token);
        println!(
            "token={} address={} first_direction={} txs={}",
            meta.symbol,
            token,
            direction.as_str(),
            n
        );
    }
    println!();
}

fn print_examples(token_meta: &BTreeMap<String, TokenMeta>, txs: &[TxAnalysis]) {
    println!("== Largest Outbound Capital Examples ==");
    let mut rows = txs
        .iter()
        .flat_map(|tx| {
            tx.flows
                .iter()
                .filter(|(_, flow)| flow.executor_out_non_collector_raw > U256::ZERO)
                .map(move |(token, flow)| (tx, token, flow))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|(_, _, a), (_, _, b)| {
        b.executor_max_out_non_collector_raw
            .cmp(&a.executor_max_out_non_collector_raw)
    });
    for (tx, token, flow) in rows.into_iter().take(80) {
        let meta = token_meta_for(token_meta, token);
        println!(
            "class={} block={} idx={} tx={} from={} executor={} token={} capital_max_out={:.8} capital_sum_out={:.8} profit_to_collector={:.8} first_direction={} first_counterparty={} gas_used={} effective_gas_price={} selector={}",
            tx.capital_class.as_str(),
            tx.block_number,
            tx.transaction_index
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            tx.tx_hash,
            tx.from_address,
            tx.executor,
            meta.symbol,
            units_to_f64(flow.executor_max_out_non_collector_raw, meta.decimals),
            units_to_f64(flow.executor_out_non_collector_raw, meta.decimals),
            units_to_f64(flow.profit_to_collector_raw, meta.decimals),
            flow.first_executor_direction
                .map(TransferDirection::as_str)
                .unwrap_or("-"),
            flow.first_executor_counterparty.as_deref().unwrap_or("-"),
            tx.gas_used
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            tx.effective_gas_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            tx.input_selector.as_deref().unwrap_or("-"),
        );
    }
}

fn analyze_tx(row: TxRow, collector: &str) -> Result<Option<TxAnalysis>> {
    let executor = row
        .to_address
        .as_deref()
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    if executor.is_empty() {
        return Ok(None);
    }

    let logs = row
        .receipt_json
        .get("logs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let flash_topics = flash_topics();
    let mut flash_evidence = Vec::new();
    let mut transfers = Vec::new();
    for log in logs {
        if let Some(evidence) = parse_flash_evidence(&log, &flash_topics)? {
            flash_evidence.push(evidence);
        }
        if let Some(transfer) = parse_transfer_log(&log)? {
            transfers.push(transfer);
        }
    }
    transfers.sort_by(|a, b| a.log_index.cmp(&b.log_index));

    let mut flows: BTreeMap<String, TokenFlow> = BTreeMap::new();
    for transfer in &transfers {
        let flow = flows.entry(transfer.token.clone()).or_default();
        if transfer.to == collector {
            flow.profit_to_collector_raw =
                flow.profit_to_collector_raw.saturating_add(transfer.amount);
        }
        if transfer.from == executor && transfer.to != collector {
            flow.executor_out_non_collector_raw = flow
                .executor_out_non_collector_raw
                .saturating_add(transfer.amount);
            if transfer.amount > flow.executor_max_out_non_collector_raw {
                flow.executor_max_out_non_collector_raw = transfer.amount;
            }
        }
        if transfer.to == executor && transfer.from != collector {
            flow.executor_in_non_collector_raw = flow
                .executor_in_non_collector_raw
                .saturating_add(transfer.amount);
        }
        flow.transfer_count += 1;
        if flow.first_executor_direction.is_none() {
            if transfer.from == executor {
                flow.first_executor_direction = Some(TransferDirection::Out);
                flow.first_executor_counterparty = Some(transfer.to.clone());
                flow.first_executor_log_index = Some(transfer.log_index);
            } else if transfer.to == executor {
                flow.first_executor_direction = Some(TransferDirection::In);
                flow.first_executor_counterparty = Some(transfer.from.clone());
                flow.first_executor_log_index = Some(transfer.log_index);
            }
        }
    }

    flows.retain(|_, flow| {
        flow.profit_to_collector_raw > U256::ZERO
            || flow.executor_out_non_collector_raw > U256::ZERO
            || flow.executor_in_non_collector_raw > U256::ZERO
    });
    if flows
        .values()
        .all(|flow| flow.profit_to_collector_raw == U256::ZERO)
    {
        return Ok(None);
    }
    let capital_class = classify_capital_source(&flash_evidence, &flows);

    Ok(Some(TxAnalysis {
        tx_hash: row.tx_hash,
        block_number: row.block_number,
        transaction_index: row.transaction_index,
        from_address: row
            .from_address
            .as_deref()
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "-".into()),
        executor,
        gas_used: parse_u128(row.gas_used.as_deref()),
        effective_gas_price: parse_u128(row.effective_gas_price.as_deref()),
        input_selector: input_selector(&row.tx_json),
        capital_class,
        flows,
        flash_evidence,
    }))
}

fn classify_capital_source(
    flash_evidence: &[FlashEvidence],
    flows: &BTreeMap<String, TokenFlow>,
) -> CapitalClass {
    if !flash_evidence.is_empty() {
        return CapitalClass::DefiniteFlashLoan;
    }
    let has_outbound = flows
        .values()
        .any(|flow| flow.executor_out_non_collector_raw > U256::ZERO);
    if !has_outbound {
        return CapitalClass::ProfitTransferOnly;
    }
    let has_outbound_first_profit_token = flows.values().any(|flow| {
        flow.profit_to_collector_raw > U256::ZERO
            && flow.executor_out_non_collector_raw > U256::ZERO
            && flow.first_executor_direction == Some(TransferDirection::Out)
    });
    if has_outbound_first_profit_token {
        return CapitalClass::PrefundedOwnCapital;
    }
    if flows.values().any(|flow| {
        flow.executor_out_non_collector_raw > U256::ZERO
            && flow.first_executor_direction == Some(TransferDirection::In)
    }) {
        return CapitalClass::InboundFirstOrBorrowed;
    }
    CapitalClass::Unclassified
}

fn parse_transfer_log(log: &Value) -> Result<Option<TransferLog>> {
    let Some(topics) = log.get("topics").and_then(Value::as_array) else {
        return Ok(None);
    };
    if topics.len() < 3 {
        return Ok(None);
    }
    let topic0 = topics[0]
        .as_str()
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    if topic0 != TRANSFER_TOPIC {
        return Ok(None);
    }
    let token = log
        .get("address")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    if token.is_empty() {
        return Ok(None);
    }
    let from = topic_to_address(topics[1].as_str().unwrap_or_default())?;
    let to = topic_to_address(topics[2].as_str().unwrap_or_default())?;
    let amount = log
        .get("data")
        .and_then(Value::as_str)
        .map(parse_u256_hex)
        .transpose()?
        .unwrap_or(U256::ZERO);
    Ok(Some(TransferLog {
        log_index: log_index(log),
        token,
        from,
        to,
        amount,
    }))
}

fn parse_flash_evidence(
    log: &Value,
    flash_topics: &BTreeMap<String, String>,
) -> Result<Option<FlashEvidence>> {
    let Some(topics) = log.get("topics").and_then(Value::as_array) else {
        return Ok(None);
    };
    let Some(topic0) = topics.first().and_then(Value::as_str) else {
        return Ok(None);
    };
    let topic0 = topic0.to_ascii_lowercase();
    let Some(label) = flash_topics.get(topic0.as_str()) else {
        return Ok(None);
    };
    Ok(Some(FlashEvidence {
        log_index: log_index(log),
        address: log
            .get("address")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .unwrap_or_else(|| "-".into()),
        topic0,
        label: label.clone(),
    }))
}

fn flash_topics() -> BTreeMap<String, String> {
    [
        (
            V3_FLASH_TOPIC.to_string(),
            "UniV3/PancakeV3/AerodromeSlipstream Flash".to_string(),
        ),
        (
            event_topic("FlashLoan(address,address,address,uint256,uint8,uint256,uint16)"),
            "Aave-style FlashLoan".to_string(),
        ),
        (
            event_topic("FlashLoan(address,address,uint256,uint256)"),
            "Balancer/ERC3156-style FlashLoan".to_string(),
        ),
        (
            event_topic("FlashLoan(address,address,uint256,uint256,address)"),
            "Extended FlashLoan".to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

async fn resolve_block_range(pool: &PgPool, cli: &Cli) -> Result<(i64, i64)> {
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT max(block_number) FROM observed_address_transfers")
            .fetch_one(pool)
            .await?;
    let to_block = cli.to_block.or(latest).unwrap_or_default();
    let from_block = cli
        .from_block
        .unwrap_or_else(|| to_block.saturating_sub(cli.days * APPROX_BASE_BLOCKS_PER_DAY));
    Ok((from_block, to_block))
}

async fn load_related_transactions(
    pool: &PgPool,
    cli: &Cli,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<TxRow>> {
    let rows = sqlx::query_as::<_, TxRow>(
        r#"
        WITH related AS (
            SELECT DISTINCT lower(t.tx_hash) AS tx_hash
            FROM observed_address_transfers t
            WHERE lower(t.seed_address) = lower($1)
              AND t.direction = 'in'
              AND t.block_number BETWEEN $2 AND $3
        )
        SELECT
            lower(ot.tx_hash) AS tx_hash,
            ot.block_number,
            ot.transaction_index,
            lower(ot.from_address) AS from_address,
            lower(ot.to_address) AS to_address,
            ot.gas_used,
            ot.effective_gas_price,
            ot.tx_json,
            ot.receipt_json
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        ORDER BY ot.block_number DESC, ot.transaction_index DESC
        LIMIT $4
        "#,
    )
    .bind(address_to_lower(cli.address))
    .bind(from_block)
    .bind(to_block)
    .bind(cli.limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

async fn load_token_meta(pool: &PgPool) -> Result<BTreeMap<String, TokenMeta>> {
    let rows = sqlx::query_as::<_, TokenRow>(
        r#"
        SELECT lower(token_address) AS token_address, symbol
        FROM tokens
        WHERE chain_id = 8453
        "#,
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let mut out = BTreeMap::new();
    for row in rows {
        let decimals = known_decimals(&row.token_address).unwrap_or(18);
        out.insert(
            row.token_address.clone(),
            TokenMeta {
                symbol: row.symbol,
                decimals,
            },
        );
    }
    for (symbol, address, decimals) in [
        ("WETH", WETH, 18),
        ("USDC", USDC, 6),
        ("cbBTC", CBBTC, 8),
        ("AERO", AERO, 18),
        ("VIRTUAL", VIRTUAL, 18),
    ] {
        out.entry(address.to_string()).or_insert_with(|| TokenMeta {
            symbol: symbol.to_string(),
            decimals,
        });
    }
    Ok(out)
}

#[derive(Debug, FromRow)]
struct TokenRow {
    token_address: String,
    symbol: String,
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut address = None;
    let mut days = 30_i64;
    let mut from_block = None;
    let mut to_block = None;
    let mut limit = 100_000_i64;
    let mut output = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--address" => {
                address = Some(
                    iter.next()
                        .context("missing value for --address")?
                        .parse::<Address>()
                        .context("invalid --address")?,
                );
            }
            "--days" => {
                days = iter
                    .next()
                    .context("missing value for --days")?
                    .parse()
                    .context("invalid --days")?;
            }
            "--from-block" => {
                from_block = Some(
                    iter.next()
                        .context("missing value for --from-block")?
                        .parse()
                        .context("invalid --from-block")?,
                );
            }
            "--to-block" => {
                to_block = Some(
                    iter.next()
                        .context("missing value for --to-block")?
                        .parse()
                        .context("invalid --to-block")?,
                );
            }
            "--limit" => {
                limit = iter
                    .next()
                    .context("missing value for --limit")?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--output" => {
                output = Some(PathBuf::from(
                    iter.next().context("missing value for --output")?,
                ));
            }
            "-h" | "--help" => {
                bail!(
                    "Usage: cargo run -p base-arb-recorder --bin competitor_capital_source -- --address <collector> [--days 30] [--from-block N] [--to-block N] [--limit 100000] [--output capital-source.txt]"
                );
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if days < 1 {
        bail!("--days must be positive");
    }
    if limit < 1 {
        bail!("--limit must be positive");
    }
    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        from_block,
        to_block,
        limit,
        output,
    })
}

fn token_meta_for(token_meta: &BTreeMap<String, TokenMeta>, token: &str) -> TokenMeta {
    token_meta.get(token).cloned().unwrap_or_else(|| TokenMeta {
        symbol: "UNKNOWN".into(),
        decimals: 18,
    })
}

fn known_decimals(address: &str) -> Option<u32> {
    match address {
        WETH => Some(18),
        USDC => Some(6),
        CBBTC => Some(8),
        AERO | VIRTUAL => Some(18),
        _ => None,
    }
}

fn input_selector(tx_json: &Value) -> Option<String> {
    let input = tx_json
        .get("input")
        .or_else(|| tx_json.get("data"))?
        .as_str()?
        .to_ascii_lowercase();
    if input.len() >= 10 {
        Some(input[..10].to_string())
    } else {
        None
    }
}

fn event_topic(signature: &str) -> String {
    format!("{:#x}", keccak256(signature.as_bytes())).to_ascii_lowercase()
}

fn log_index(log: &Value) -> i64 {
    hex_i64(log.get("logIndex").and_then(Value::as_str)).unwrap_or_default()
}

fn topic_to_address(topic: &str) -> Result<String> {
    let raw = topic.trim_start_matches("0x");
    if raw.len() < 40 {
        bail!("invalid address topic: {topic}");
    }
    Ok(format!("0x{}", &raw[raw.len() - 40..]).to_ascii_lowercase())
}

fn parse_u256_hex(value: &str) -> Result<U256> {
    Ok(U256::from_str_radix(value.trim_start_matches("0x"), 16)?)
}

fn parse_u128(value: Option<&str>) -> Option<u128> {
    value.and_then(|value| value.parse::<u128>().ok())
}

fn hex_i64(raw: Option<&str>) -> Option<i64> {
    let raw = raw?;
    i64::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

fn address_to_lower(address: Address) -> String {
    format!("{address:#x}").to_ascii_lowercase()
}

fn units_to_f64(value: U256, decimals: u32) -> f64 {
    let raw = value.to_string().parse::<f64>().unwrap_or_default();
    raw / 10_f64.powi(decimals as i32)
}

fn percentile(mut values: Vec<f64>, p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = ((values.len().saturating_sub(1) as f64) * p).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn max_value(values: Vec<f64>) -> f64 {
    values
        .into_iter()
        .fold(0.0, |acc, value| if value > acc { value } else { acc })
}

fn set_report_output(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let _ = REPORT_OUTPUT.set(Mutex::new(Some(file)));
    Ok(())
}

fn write_report_line(args: std::fmt::Arguments<'_>) {
    std::println!("{args}");
    if let Some(lock) = REPORT_OUTPUT.get() {
        if let Ok(mut guard) = lock.lock() {
            if let Some(file) = guard.as_mut() {
                let _ = writeln!(file, "{args}");
            }
        }
    }
}
