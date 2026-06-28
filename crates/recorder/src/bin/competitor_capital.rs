use std::{
    cmp::Ordering,
    collections::BTreeMap,
    env,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use alloy_primitives::{Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const WETH: &str = "0x4200000000000000000000000000000000000006";
const USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
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
    shell_output: bool,
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
    receipt_json: Value,
}

#[derive(Debug, Clone)]
struct TokenFlow {
    token: &'static TokenMeta,
    profit_to_collector_raw: U256,
    executor_out_non_collector_raw: U256,
    executor_max_out_non_collector_raw: U256,
    executor_in_non_collector_raw: U256,
    transfer_count: usize,
}

#[derive(Debug, Clone)]
struct TxCapital {
    tx_hash: String,
    block_number: i64,
    transaction_index: Option<i64>,
    from_address: String,
    executor: String,
    gas_used: Option<u128>,
    effective_gas_price: Option<u128>,
    token_flows: Vec<TokenFlow>,
}

#[derive(Debug, Clone, Copy)]
struct TokenMeta {
    symbol: &'static str,
    address: &'static str,
    decimals: u32,
}

const TOKENS: [TokenMeta; 2] = [
    TokenMeta {
        symbol: "WETH",
        address: WETH,
        decimals: 18,
    },
    TokenMeta {
        symbol: "USDC",
        address: USDC,
        decimals: 6,
    },
];

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
    let rows = load_related_transactions(&store.pool, &cli, from_block, to_block).await?;
    let collector = address_to_lower(cli.address);
    let txs = rows
        .into_iter()
        .filter_map(|row| analyze_tx(row, &collector).transpose())
        .collect::<Result<Vec<_>>>()?;

    if cli.shell_output {
        print_shell_exports(&cli, from_block, to_block, &txs);
    } else {
        print_report(&cli, from_block, to_block, &txs);
    }
    Ok(())
}

fn print_shell_exports(cli: &Cli, from_block: i64, to_block: i64, txs: &[TxCapital]) {
    println!("SHADOW_SCOPE_ADDRESS={}", address_to_lower(cli.address));
    println!("SHADOW_SCOPE_FROM_BLOCK={from_block}");
    println!("SHADOW_SCOPE_TO_BLOCK={to_block}");
    println!("SHADOW_SCOPE_TXS={}", txs.len());
    for token in TOKENS {
        let flows = txs
            .iter()
            .flat_map(|tx| tx.token_flows.iter())
            .filter(|flow| flow.token.address == token.address)
            .filter(|flow| {
                flow.profit_to_collector_raw > U256::ZERO
                    && flow.executor_out_non_collector_raw > U256::ZERO
            })
            .cloned()
            .collect::<Vec<_>>();
        let raw_values = flows
            .iter()
            .map(|flow| flow.executor_max_out_non_collector_raw)
            .collect::<Vec<_>>();
        let prefix = format!("SHADOW_{}", token.symbol);
        println!("{}_TXS={}", prefix, flows.len());
        println!(
            "{}_P50_RAW={}",
            prefix,
            percentile_u256(raw_values.clone(), 0.50)
        );
        println!(
            "{}_P90_RAW={}",
            prefix,
            percentile_u256(raw_values.clone(), 0.90)
        );
        println!(
            "{}_P99_RAW={}",
            prefix,
            percentile_u256(raw_values.clone(), 0.99)
        );
        println!("{}_MAX_RAW={}", prefix, max_u256(raw_values));
    }
}

fn print_report(cli: &Cli, from_block: i64, to_block: i64, txs: &[TxCapital]) {
    println!("== Scope ==");
    println!("collector: {}", address_to_lower(cli.address));
    println!("days: {}", cli.days);
    println!("blocks: {from_block}..{to_block}");
    println!("related_txs_with_weth_or_usdc_profit: {}", txs.len());
    println!();

    print_upstream_summary(txs);
    print_token_summary(txs);
    print_examples(txs);
}

fn print_upstream_summary(txs: &[TxCapital]) {
    println!("== Upstream Execution Addresses ==");
    let mut by_lane: BTreeMap<(String, String), usize> = BTreeMap::new();
    for tx in txs {
        *by_lane
            .entry((tx.from_address.clone(), tx.executor.clone()))
            .or_default() += 1;
    }
    let mut rows = by_lane.into_iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1));
    for ((from, executor), n) in rows.into_iter().take(20) {
        println!("from={from} executor={executor} txs={n}");
    }
    println!();
}

fn print_token_summary(txs: &[TxCapital]) {
    println!("== Capital Distribution By Token ==");
    for token in TOKENS {
        let flows = txs
            .iter()
            .flat_map(|tx| tx.token_flows.iter())
            .filter(|flow| flow.token.address == token.address)
            .filter(|flow| {
                flow.profit_to_collector_raw > U256::ZERO
                    && flow.executor_out_non_collector_raw > U256::ZERO
            })
            .cloned()
            .collect::<Vec<_>>();
        if flows.is_empty() {
            println!(
                "{}: no txs with profit and executor outbound capital",
                token.symbol
            );
            continue;
        }

        let max_out = flows
            .iter()
            .map(|flow| units_to_f64(flow.executor_max_out_non_collector_raw, token.decimals))
            .collect::<Vec<_>>();
        let sum_out = flows
            .iter()
            .map(|flow| units_to_f64(flow.executor_out_non_collector_raw, token.decimals))
            .collect::<Vec<_>>();
        let profit = flows
            .iter()
            .map(|flow| units_to_f64(flow.profit_to_collector_raw, token.decimals))
            .collect::<Vec<_>>();
        let returns_bps = flows
            .iter()
            .filter_map(|flow| {
                if flow.executor_max_out_non_collector_raw == U256::ZERO {
                    None
                } else {
                    Some(
                        units_to_f64(flow.profit_to_collector_raw, token.decimals)
                            / units_to_f64(flow.executor_max_out_non_collector_raw, token.decimals)
                            * 10_000.0,
                    )
                }
            })
            .collect::<Vec<_>>();

        println!(
            "{} txs={} capital_max_out p50={:.8} p90={:.8} p99={:.8} max={:.8} | capital_sum_out p50={:.8} p90={:.8} | profit p50={:.8} p90={:.8} | profit/capital_max_out_bps p50={:.4} p90={:.4}",
            token.symbol,
            flows.len(),
            percentile(max_out.clone(), 0.50),
            percentile(max_out.clone(), 0.90),
            percentile(max_out.clone(), 0.99),
            max_value(max_out),
            percentile(sum_out.clone(), 0.50),
            percentile(sum_out, 0.90),
            percentile(profit.clone(), 0.50),
            percentile(profit, 0.90),
            percentile(returns_bps.clone(), 0.50),
            percentile(returns_bps, 0.90),
        );
    }
    println!();

    let gas_costs = txs
        .iter()
        .filter_map(|tx| Some((tx.gas_used? as f64) * (tx.effective_gas_price? as f64) / 1e18))
        .collect::<Vec<_>>();
    if !gas_costs.is_empty() {
        println!(
            "== Gas Cost ETH ==\ntxs={} p50={:.10} p90={:.10} p99={:.10} max={:.10}\n",
            gas_costs.len(),
            percentile(gas_costs.clone(), 0.50),
            percentile(gas_costs.clone(), 0.90),
            percentile(gas_costs.clone(), 0.99),
            max_value(gas_costs),
        );
    }
}

fn print_examples(txs: &[TxCapital]) {
    println!("== Largest WETH/USDC Capital Examples ==");
    let mut rows = txs
        .iter()
        .flat_map(|tx| {
            tx.token_flows
                .iter()
                .filter(|flow| flow.executor_out_non_collector_raw > U256::ZERO)
                .map(move |flow| (tx, flow))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|(_, a), (_, b)| {
        b.executor_max_out_non_collector_raw
            .cmp(&a.executor_max_out_non_collector_raw)
    });
    for (tx, flow) in rows.into_iter().take(40) {
        println!(
            "block={} idx={} tx={} from={} executor={} token={} capital_max_out={:.8} capital_sum_out={:.8} profit_to_collector={:.8} gas_used={} effective_gas_price={}",
            tx.block_number,
            tx.transaction_index
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            tx.tx_hash,
            tx.from_address,
            tx.executor,
            flow.token.symbol,
            units_to_f64(flow.executor_max_out_non_collector_raw, flow.token.decimals),
            units_to_f64(flow.executor_out_non_collector_raw, flow.token.decimals),
            units_to_f64(flow.profit_to_collector_raw, flow.token.decimals),
            tx.gas_used
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            tx.effective_gas_price
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
        );
    }
}

fn analyze_tx(row: TxRow, collector: &str) -> Result<Option<TxCapital>> {
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
    let mut flows = TOKENS
        .iter()
        .map(|token| {
            (
                token.address,
                TokenFlow {
                    token,
                    profit_to_collector_raw: U256::ZERO,
                    executor_out_non_collector_raw: U256::ZERO,
                    executor_max_out_non_collector_raw: U256::ZERO,
                    executor_in_non_collector_raw: U256::ZERO,
                    transfer_count: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    for log in logs {
        let Some((token, from, to, amount)) = parse_transfer_log(&log)? else {
            continue;
        };
        let Some(flow) = flows.get_mut(token.as_str()) else {
            continue;
        };
        flow.transfer_count += 1;
        if to == collector {
            flow.profit_to_collector_raw = flow.profit_to_collector_raw.saturating_add(amount);
        }
        if from == executor && to != collector {
            flow.executor_out_non_collector_raw =
                flow.executor_out_non_collector_raw.saturating_add(amount);
            if amount > flow.executor_max_out_non_collector_raw {
                flow.executor_max_out_non_collector_raw = amount;
            }
        }
        if to == executor && from != collector {
            flow.executor_in_non_collector_raw =
                flow.executor_in_non_collector_raw.saturating_add(amount);
        }
    }

    let token_flows = flows
        .into_values()
        .filter(|flow| flow.profit_to_collector_raw > U256::ZERO)
        .collect::<Vec<_>>();
    if token_flows.is_empty() {
        return Ok(None);
    }

    Ok(Some(TxCapital {
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
        token_flows,
    }))
}

fn parse_transfer_log(log: &Value) -> Result<Option<(String, String, String, U256)>> {
    let address = log
        .get("address")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_default();
    if address != WETH && address != USDC {
        return Ok(None);
    }
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
    let from = topic_to_address(topics[1].as_str().unwrap_or_default())?;
    let to = topic_to_address(topics[2].as_str().unwrap_or_default())?;
    let amount = log
        .get("data")
        .and_then(Value::as_str)
        .map(parse_u256_hex)
        .transpose()?
        .unwrap_or(U256::ZERO);
    Ok(Some((address, from, to, amount)))
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
              AND lower(t.token_address) IN (lower($2), lower($3))
              AND t.block_number BETWEEN $4 AND $5
        )
        SELECT
            lower(ot.tx_hash) AS tx_hash,
            ot.block_number,
            ot.transaction_index,
            lower(ot.from_address) AS from_address,
            lower(ot.to_address) AS to_address,
            ot.gas_used,
            ot.effective_gas_price,
            ot.receipt_json
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        ORDER BY ot.block_number DESC, ot.transaction_index DESC
        LIMIT $6
        "#,
    )
    .bind(address_to_lower(cli.address))
    .bind(WETH)
    .bind(USDC)
    .bind(from_block)
    .bind(to_block)
    .bind(cli.limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut address = None;
    let mut days = 30_i64;
    let mut from_block = None;
    let mut to_block = None;
    let mut limit = 50_000_i64;
    let mut output = None;
    let mut shell_output = false;
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
            "--shell" => {
                shell_output = true;
            }
            "-h" | "--help" => {
                bail!(
                    "Usage: cargo run -p base-arb-recorder --bin competitor_capital -- --address <collector> [--days 30] [--from-block N] [--to-block N] [--limit 50000] [--output capital.txt] [--shell]"
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
        shell_output,
    })
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

fn percentile_u256(mut values: Vec<U256>, p: f64) -> U256 {
    if values.is_empty() {
        return U256::ZERO;
    }
    values.sort();
    let idx = ((values.len().saturating_sub(1) as f64) * p).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn max_u256(values: Vec<U256>) -> U256 {
    values.into_iter().max().unwrap_or(U256::ZERO)
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
