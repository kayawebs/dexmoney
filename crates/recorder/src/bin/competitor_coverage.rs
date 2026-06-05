use std::{
    env,
    fmt::Arguments,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use alloy_primitives::{Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use sqlx::{PgPool, Row};
use tracing_subscriber::EnvFilter;

const UNI_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;

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
    from_block: Option<u64>,
    to_block: Option<u64>,
    min_txs: i64,
    limit: i64,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct SwapPoolRow {
    pool_address: String,
    topic0: String,
    swap_family: String,
    swap_logs: i64,
    txs: i64,
    first_block: i64,
    latest_block: i64,
    registry_symbol: Option<String>,
    registry_dex: Option<String>,
    registry_variant: Option<String>,
    registry_enabled: Option<bool>,
    pair_enabled: Option<bool>,
    latest_state_block: Option<i64>,
    latest_state_source: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedPool {
    pool: Address,
    family: String,
    token0: Option<Address>,
    token1: Option<Address>,
    token0_symbol: Option<String>,
    token1_symbol: Option<String>,
    factory: Option<Address>,
    fee_pips: Option<u32>,
    fee_bps: Option<u32>,
    tick_spacing: Option<i32>,
    stable: Option<bool>,
    inferred_dex: String,
    inferred_variant: String,
    resolve_error: Option<String>,
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
    let provider = ChainProvider::from_settings(&settings);
    let (from_block, to_block) = resolve_block_range(&provider, &cli).await?;

    print_scope(&cli, from_block, to_block).await?;
    print_counterparty_summary(&store.pool, &cli, from_block, to_block).await?;
    print_pool_coverage(
        &store.pool,
        &provider,
        &settings,
        &cli,
        from_block,
        to_block,
    )
    .await?;

    if let Some(path) = cli.output.as_deref() {
        flush_report_output()?;
        std::println!("coverage report written to {}", path.display());
    }
    Ok(())
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut address = None;
    let mut days = 30_i64;
    let mut from_block = None;
    let mut to_block = None;
    let mut min_txs = 10_i64;
    let mut limit = 100_i64;
    let mut output = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--address" => {
                address = Some(
                    Address::from_str(&iter.next().context("missing value for --address")?)
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
            "--min-txs" => {
                min_txs = iter
                    .next()
                    .context("missing value for --min-txs")?
                    .parse()
                    .context("invalid --min-txs")?;
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
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if min_txs < 1 {
        bail!("--min-txs must be positive");
    }
    if limit < 1 {
        bail!("--limit must be positive");
    }

    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        from_block,
        to_block,
        min_txs,
        limit,
        output,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_coverage -- --address <addr> [--days 30] [--from-block N] [--to-block N] [--min-txs 10] [--limit 100] [--output report.txt]"
    );
}

async fn resolve_block_range(provider: &ChainProvider, cli: &Cli) -> Result<(u64, u64)> {
    let latest = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest);
    let from_block = cli.from_block.unwrap_or_else(|| {
        to_block.saturating_sub((cli.days.max(0) as u64).saturating_mul(APPROX_BASE_BLOCKS_PER_DAY))
    });
    if from_block > to_block {
        bail!("from block {from_block} is greater than to block {to_block}");
    }
    Ok((from_block, to_block))
}

async fn print_scope(cli: &Cli, from_block: u64, to_block: u64) -> Result<()> {
    println!("== Scope ==");
    println!("address: {:#x}", cli.address);
    println!("blocks: {from_block}..{to_block}");
    println!(
        "days: {} min_txs: {} limit: {}",
        cli.days, cli.min_txs, cli.limit
    );
    println!();
    Ok(())
}

async fn print_counterparty_summary(
    pool: &PgPool,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = sqlx::query(
        r#"
        SELECT
            direction,
            counterparty,
            COUNT(DISTINCT tx_hash)::bigint AS txs,
            COUNT(*)::bigint AS transfers,
            MIN(block_number)::bigint AS first_block,
            MAX(block_number)::bigint AS latest_block
        FROM observed_address_transfers
        WHERE lower(seed_address) = lower($1)
          AND block_number BETWEEN $2 AND $3
        GROUP BY direction, counterparty
        ORDER BY txs DESC, transfers DESC
        LIMIT 20
        "#,
    )
    .bind(address)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .fetch_all(pool)
    .await?;

    println!("== Counterparty Summary ==");
    if rows.is_empty() {
        println!("no cached transfers for seed address; run competitor_report --hydrate-all first");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "{} counterparty={} txs={} transfers={} blocks={}..{}",
            cell_string(&row, "direction").unwrap_or_else(|| "-".into()),
            cell_string(&row, "counterparty").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "txs").unwrap_or_default(),
            cell_i64(&row, "transfers").unwrap_or_default(),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
        );
    }
    println!();
    Ok(())
}

async fn print_pool_coverage(
    pool: &PgPool,
    provider: &ChainProvider,
    settings: &Settings,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows =
        fetch_swap_pools(pool, &address, from_block, to_block, cli.min_txs, cli.limit).await?;

    println!("== Competitor Pool Coverage ==");
    if rows.is_empty() {
        println!(
            "no swap logs in cached related receipts; run competitor_report --hydrate-all first"
        );
        println!();
        return Ok(());
    }

    let mut registered = 0_i64;
    let mut enabled = 0_i64;
    let mut ready = 0_i64;
    for row in &rows {
        if row.registry_dex.is_some() {
            registered += 1;
        }
        if row.registry_enabled == Some(true) && row.pair_enabled == Some(true) {
            enabled += 1;
        }
        if row.registry_enabled == Some(true)
            && row.pair_enabled == Some(true)
            && row.latest_state_block.is_some()
        {
            ready += 1;
        }
    }
    println!(
        "pools={} registered={} enabled_pair_and_pool={} with_latest_state={}",
        rows.len(),
        registered,
        enabled,
        ready,
    );

    for row in rows {
        let pool_address = Address::from_str(&row.pool_address)
            .with_context(|| format!("invalid pool address {}", row.pool_address))?;
        let resolved = resolve_pool(
            provider,
            settings,
            pool_address,
            &row.topic0,
            &row.swap_family,
        )
        .await;
        let resolved = match resolved {
            Ok(value) => value,
            Err(err) => ResolvedPool {
                pool: pool_address,
                family: row.swap_family.clone(),
                token0: None,
                token1: None,
                token0_symbol: None,
                token1_symbol: None,
                factory: None,
                fee_pips: None,
                fee_bps: None,
                tick_spacing: None,
                stable: None,
                inferred_dex: "-".into(),
                inferred_variant: "-".into(),
                resolve_error: Some(err.to_string()),
            },
        };

        let pair = match (
            resolved.token0_symbol.as_deref(),
            resolved.token1_symbol.as_deref(),
        ) {
            (Some(token0), Some(token1)) => format!("{token0}/{token1}"),
            _ => "-".into(),
        };
        let status = coverage_status(&row);
        println!(
            "status={} pool={:#x} txs={} logs={} blocks={}..{} family={} pair={} token0={} token1={} factory={} inferred={}:{} fee_pips={} fee_bps={} tick_spacing={} stable={} registry_symbol={} registry={}:{} pair_enabled={} pool_enabled={} state_block={} state_source={} error={}",
            status,
            resolved.pool,
            row.txs,
            row.swap_logs,
            row.first_block,
            row.latest_block,
            resolved.family,
            pair,
            fmt_addr(resolved.token0),
            fmt_addr(resolved.token1),
            fmt_addr(resolved.factory),
            resolved.inferred_dex,
            resolved.inferred_variant,
            fmt_opt_u32(resolved.fee_pips),
            fmt_opt_u32(resolved.fee_bps),
            fmt_opt_i32(resolved.tick_spacing),
            fmt_opt_bool(resolved.stable),
            row.registry_symbol.as_deref().unwrap_or("-"),
            row.registry_dex.as_deref().unwrap_or("-"),
            row.registry_variant.as_deref().unwrap_or("-"),
            fmt_opt_bool(row.pair_enabled),
            fmt_opt_bool(row.registry_enabled),
            row.latest_state_block
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            row.latest_state_source.as_deref().unwrap_or("-"),
            resolved.resolve_error.as_deref().unwrap_or("-"),
        );
    }
    println!();
    Ok(())
}

async fn fetch_swap_pools(
    pool: &PgPool,
    address: &str,
    from_block: u64,
    to_block: u64,
    min_txs: i64,
    limit: i64,
) -> Result<Vec<SwapPoolRow>> {
    let rows = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
        ),
        swap_logs AS (
            SELECT
                lower(log->>'address') AS pool_address,
                lower(log->'topics'->>0) AS topic0,
                ot.tx_hash,
                ot.block_number
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            WHERE lower(log->'topics'->>0) = ANY($4)
              AND ot.block_number BETWEEN $2 AND $3
        ),
        latest_state AS (
            SELECT DISTINCT ON (lower(pool_address))
                lower(pool_address) AS pool_address,
                block_number,
                source
            FROM pool_states
            ORDER BY lower(pool_address), block_number DESC, updated_at DESC
        )
        SELECT
            sl.pool_address,
            sl.topic0,
            CASE sl.topic0
                WHEN $5 THEN 'v3/slipstream'
                WHEN $6 THEN 'pancake-v3'
                WHEN $7 THEN 'classic-v2'
                ELSE sl.topic0
            END AS swap_family,
            COUNT(*)::bigint AS swap_logs,
            COUNT(DISTINCT sl.tx_hash)::bigint AS txs,
            MIN(sl.block_number)::bigint AS first_block,
            MAX(sl.block_number)::bigint AS latest_block,
            tp.symbol AS registry_symbol,
            p.dex AS registry_dex,
            p.variant AS registry_variant,
            p.enabled AS registry_enabled,
            tp.enabled AS pair_enabled,
            ls.block_number AS latest_state_block,
            ls.source AS latest_state_source
        FROM swap_logs sl
        LEFT JOIN pools p ON lower(p.pool_address) = sl.pool_address
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        LEFT JOIN latest_state ls ON ls.pool_address = sl.pool_address
        GROUP BY
            sl.pool_address, sl.topic0, tp.symbol, p.dex, p.variant, p.enabled,
            tp.enabled, ls.block_number, ls.source
        HAVING COUNT(DISTINCT sl.tx_hash) >= $8
        ORDER BY COUNT(DISTINCT sl.tx_hash) DESC, COUNT(*) DESC
        LIMIT $9
        "#,
    )
    .bind(address)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .bind(vec![
        UNI_V3_SWAP_TOPIC.to_string(),
        PANCAKE_V3_SWAP_TOPIC.to_string(),
        CLASSIC_SWAP_TOPIC.to_string(),
    ])
    .bind(UNI_V3_SWAP_TOPIC)
    .bind(PANCAKE_V3_SWAP_TOPIC)
    .bind(CLASSIC_SWAP_TOPIC)
    .bind(min_txs)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| SwapPoolRow {
            pool_address: cell_string(&row, "pool_address").unwrap_or_default(),
            topic0: cell_string(&row, "topic0").unwrap_or_default(),
            swap_family: cell_string(&row, "swap_family").unwrap_or_default(),
            swap_logs: cell_i64(&row, "swap_logs").unwrap_or_default(),
            txs: cell_i64(&row, "txs").unwrap_or_default(),
            first_block: cell_i64(&row, "first_block").unwrap_or_default(),
            latest_block: cell_i64(&row, "latest_block").unwrap_or_default(),
            registry_symbol: cell_string(&row, "registry_symbol"),
            registry_dex: cell_string(&row, "registry_dex"),
            registry_variant: cell_string(&row, "registry_variant"),
            registry_enabled: cell_bool(&row, "registry_enabled"),
            pair_enabled: cell_bool(&row, "pair_enabled"),
            latest_state_block: cell_i64(&row, "latest_state_block"),
            latest_state_source: cell_string(&row, "latest_state_source"),
        })
        .collect())
}

async fn resolve_pool(
    provider: &ChainProvider,
    settings: &Settings,
    pool: Address,
    topic0: &str,
    family: &str,
) -> Result<ResolvedPool> {
    let token0 = call_address(provider, pool, "0x0dfe1681", "token0()").await?;
    let token1 = call_address(provider, pool, "0xd21220a7", "token1()").await?;
    let token0_symbol = provider.fetch_erc20_symbol(token0).await.ok();
    let token1_symbol = provider.fetch_erc20_symbol(token1).await.ok();
    let factory = call_address(provider, pool, "0xc45a0155", "factory()")
        .await
        .ok();

    let mut fee_pips = None;
    let fee_bps;
    let mut tick_spacing = None;
    let mut stable = None;
    let (mut inferred_dex, mut inferred_variant) = infer_protocol(settings, topic0, factory);

    if topic0 == CLASSIC_SWAP_TOPIC {
        stable = call_bool(provider, pool, "0x22be3de1", "stable()")
            .await
            .ok();
        inferred_variant = if stable.unwrap_or(false) {
            "AerodromeStable".into()
        } else {
            "AerodromeVolatile".into()
        };
        fee_bps = call_u32(provider, pool, "0x30adf81f", "fee()").await.ok();
    } else {
        fee_pips = call_u32(provider, pool, "0xddca3f43", "fee()").await.ok();
        tick_spacing = call_i24(provider, pool, "0xd0c93a7c", "tickSpacing()")
            .await
            .ok();
        if fee_pips.is_none() {
            if let Some(factory) = factory {
                fee_pips = call_get_swap_fee(provider, factory, pool).await.ok();
            }
        }
        fee_bps = fee_pips.map(|fee| fee / 100);
    }

    if topic0 == UNI_V3_SWAP_TOPIC && inferred_variant == "UniswapV3" {
        if factory == settings.aerodrome_slipstream_factory {
            inferred_dex = "Aerodrome".into();
            inferred_variant = "AerodromeSlipstream".into();
        }
    }

    Ok(ResolvedPool {
        pool,
        family: family.into(),
        token0: Some(token0),
        token1: Some(token1),
        token0_symbol,
        token1_symbol,
        factory,
        fee_pips,
        fee_bps,
        tick_spacing,
        stable,
        inferred_dex,
        inferred_variant,
        resolve_error: None,
    })
}

fn infer_protocol(settings: &Settings, topic0: &str, factory: Option<Address>) -> (String, String) {
    if topic0 == PANCAKE_V3_SWAP_TOPIC {
        return ("PancakeSwap".into(), "PancakeV3".into());
    }
    if topic0 == CLASSIC_SWAP_TOPIC {
        return ("Aerodrome".into(), "AerodromeVolatile".into());
    }
    if factory == settings.aerodrome_slipstream_factory {
        return ("Aerodrome".into(), "AerodromeSlipstream".into());
    }
    if factory == settings.uniswap_v3_factory {
        return ("UniswapV3".into(), "UniswapV3".into());
    }
    ("UniswapV3".into(), "UniswapV3".into())
}

async fn call_address(
    provider: &ChainProvider,
    to: Address,
    data: &str,
    label: &str,
) -> Result<Address> {
    let raw = provider.eth_call_from(None, to, data, label).await?;
    let words = decode_32byte_words(&raw)?;
    parse_word_address(&words[0])
}

async fn call_u32(provider: &ChainProvider, to: Address, data: &str, label: &str) -> Result<u32> {
    let raw = provider.eth_call_from(None, to, data, label).await?;
    let words = decode_32byte_words(&raw)?;
    let value = parse_word_u256(&words[0])?;
    u32::try_from(value).map_err(|_| anyhow::anyhow!("{label} value too large: {value}"))
}

async fn call_i24(provider: &ChainProvider, to: Address, data: &str, label: &str) -> Result<i32> {
    let raw = provider.eth_call_from(None, to, data, label).await?;
    let words = decode_32byte_words(&raw)?;
    parse_word_i24(&words[0])
}

async fn call_bool(provider: &ChainProvider, to: Address, data: &str, label: &str) -> Result<bool> {
    let raw = provider.eth_call_from(None, to, data, label).await?;
    let words = decode_32byte_words(&raw)?;
    Ok(!parse_word_u256(&words[0])?.is_zero())
}

async fn call_get_swap_fee(
    provider: &ChainProvider,
    factory: Address,
    pool: Address,
) -> Result<u32> {
    let raw = provider
        .eth_call_from(
            None,
            factory,
            &format!("0x35458dcc{}", encode_address_word(pool)),
            "Aerodrome Slipstream getSwapFee(address)",
        )
        .await?;
    let words = decode_32byte_words(&raw)?;
    let value = parse_word_u256(&words[0])?;
    u32::try_from(value).map_err(|_| anyhow::anyhow!("getSwapFee value too large: {value}"))
}

fn decode_32byte_words(data: &str) -> Result<Vec<String>> {
    let clean = data.trim_start_matches("0x");
    if clean.is_empty() {
        bail!("empty eth_call result");
    }
    if !clean.len().is_multiple_of(64) {
        bail!("unexpected eth_call word length");
    }
    Ok((0..clean.len())
        .step_by(64)
        .map(|index| clean[index..index + 64].to_string())
        .collect())
}

fn parse_word_u256(word: &str) -> Result<U256> {
    Ok(U256::from_str_radix(word, 16)?)
}

fn parse_word_address(word: &str) -> Result<Address> {
    let address_start = word
        .len()
        .checked_sub(40)
        .ok_or_else(|| anyhow::anyhow!("abi word too short for address"))?;
    Ok(format!("0x{}", &word[address_start..]).parse()?)
}

fn parse_word_i24(word: &str) -> Result<i32> {
    let low = u32::from_str_radix(&word[word.len() - 6..], 16)?;
    let value = if (low & 0x800000) != 0 {
        (low as i32) | !0x00ff_ffff
    } else {
        low as i32
    };
    Ok(value)
}

fn encode_address_word(address: Address) -> String {
    let clean = format!("{address:#x}").trim_start_matches("0x").to_string();
    format!("{clean:0>64}")
}

fn coverage_status(row: &SwapPoolRow) -> &'static str {
    match (
        row.registry_enabled,
        row.pair_enabled,
        row.latest_state_block.is_some(),
    ) {
        (Some(true), Some(true), true) => "ready",
        (Some(true), Some(true), false) => "missing_state",
        (Some(false), _, _) | (_, Some(false), _) => "disabled",
        (Some(_), _, _) => "registered_incomplete",
        _ => "missing_registry",
    }
}

fn set_report_output(path: &Path) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("failed to create report output {}", path.display()))?;
    let lock = REPORT_OUTPUT.get_or_init(|| Mutex::new(None));
    *lock
        .lock()
        .map_err(|_| anyhow::anyhow!("report output mutex poisoned"))? = Some(file);
    Ok(())
}

fn write_report_line(args: Arguments<'_>) {
    let lock = REPORT_OUTPUT.get_or_init(|| Mutex::new(None));
    let Ok(mut guard) = lock.lock() else {
        std::eprintln!("report output mutex poisoned");
        return;
    };
    if let Some(file) = guard.as_mut() {
        if let Err(err) = writeln!(file, "{args}") {
            std::eprintln!("failed to write report output: {err}");
        }
    } else {
        std::println!("{args}");
    }
}

fn flush_report_output() -> Result<()> {
    let lock = REPORT_OUTPUT.get_or_init(|| Mutex::new(None));
    let mut guard = lock
        .lock()
        .map_err(|_| anyhow::anyhow!("report output mutex poisoned"))?;
    if let Some(file) = guard.as_mut() {
        file.flush().context("failed to flush report output")?;
    }
    Ok(())
}

fn cell_string(row: &sqlx::postgres::PgRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn cell_i64(row: &sqlx::postgres::PgRow, name: &str) -> Option<i64> {
    row.try_get::<Option<i64>, _>(name).ok().flatten()
}

fn cell_bool(row: &sqlx::postgres::PgRow, name: &str) -> Option<bool> {
    row.try_get::<Option<bool>, _>(name).ok().flatten()
}

fn fmt_addr(address: Option<Address>) -> String {
    address
        .map(|value| format!("{value:#x}"))
        .unwrap_or_else(|| "-".into())
}

fn fmt_opt_u32(value: Option<u32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}

fn fmt_opt_i32(value: Option<i32>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}

fn fmt_opt_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".into())
}
