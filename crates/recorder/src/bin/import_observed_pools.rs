use std::{
    env,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Mutex, OnceLock},
};

use alloy_primitives::Address;
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, DiscoveredPool, PoolVariant},
};
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
    min_pool_txs: i64,
    pool_limit: i64,
    apply: bool,
    output: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct PoolHit {
    pool_address: String,
    topic0: String,
    family: String,
    swap_logs: i64,
    txs: i64,
    first_block: i64,
    latest_block: i64,
    registry_symbol: Option<String>,
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
    let rows = fetch_pool_hits(&store.pool, &cli, from_block, to_block).await?;

    println!("== Observed Pool Import ==");
    println!("address: {:#x}", cli.address);
    println!("blocks: {from_block}..{to_block}");
    println!(
        "days: {} min_pool_txs: {} pool_limit: {} mode: {}",
        cli.days,
        cli.min_pool_txs,
        cli.pool_limit,
        if cli.apply { "apply" } else { "dry-run" }
    );
    println!();

    let mut imported = 0usize;
    let mut observed_only = 0usize;
    let mut already_registered = 0usize;
    let mut failed = 0usize;

    for row in rows {
        let pool_address = Address::from_str(&row.pool_address)
            .with_context(|| format!("invalid pool address {}", row.pool_address))?;
        if row.registry_symbol.is_some() {
            already_registered += 1;
        }

        match provider
            .resolve_observed_pool_for_registry(&settings, pool_address, &row.topic0)
            .await
        {
            Ok(discovered) => {
                let symbol =
                    pair_symbol(&provider, discovered.state.token0, discovered.state.token1).await;
                let dex = dex_to_string(discovered.state.dex);
                let variant = variant_to_string(discovered.state.variant);
                if cli.apply {
                    upsert_token_registry(
                        &store.pool,
                        settings.chain_id,
                        discovered.state.token0,
                        symbol.split('/').next().unwrap_or_default(),
                    )
                    .await?;
                    upsert_token_registry(
                        &store.pool,
                        settings.chain_id,
                        discovered.state.token1,
                        symbol.split('/').nth(1).unwrap_or_default(),
                    )
                    .await?;
                    let (token0, token1) =
                        canonical_pair(discovered.state.token0, discovered.state.token1);
                    let pair_id = store
                        .upsert_token_pair(settings.chain_id, token0, token1, &symbol)
                        .await?;
                    store.upsert_discovered_pool(pair_id, &discovered).await?;
                    upsert_observed(
                        &store,
                        settings.chain_id,
                        &row,
                        pool_address,
                        Some(&discovered),
                        Some(&symbol),
                        "imported",
                        None,
                    )
                    .await?;
                }
                imported += 1;
                println!(
                    "importable pool={pool_address:#x} pair={symbol} dex={dex} variant={variant} txs={} logs={} registered={} source={}",
                    row.txs,
                    row.swap_logs,
                    row.registry_symbol.as_deref().unwrap_or("-"),
                    discovered.source,
                );
            }
            Err(err) => {
                observed_only += 1;
                if cli.apply {
                    let metadata = provider
                        .resolve_observed_pool_metadata(pool_address, &row.topic0)
                        .await
                        .ok();
                    let symbol = match metadata.as_ref() {
                        Some(metadata) => {
                            Some(pair_symbol(&provider, metadata.token0, metadata.token1).await)
                        }
                        None => None,
                    };
                    store
                        .upsert_observed_pool(
                            settings.chain_id,
                            pool_address,
                            &row.topic0,
                            &row.family,
                            metadata.as_ref().map(|metadata| metadata.token0),
                            metadata.as_ref().map(|metadata| metadata.token1),
                            symbol.as_deref(),
                            metadata
                                .as_ref()
                                .and_then(|metadata| metadata.factory_address),
                            None,
                            None,
                            metadata.as_ref().and_then(|metadata| metadata.fee_bps),
                            metadata.as_ref().and_then(|metadata| metadata.fee_pips),
                            metadata.as_ref().and_then(|metadata| metadata.tick_spacing),
                            metadata.as_ref().and_then(|metadata| metadata.stable),
                            row.txs,
                            row.swap_logs,
                            Some(row.first_block),
                            Some(row.latest_block),
                            "manual_import",
                            "observed_only",
                            Some(&err.to_string()),
                        )
                        .await?;
                }
                failed += 1;
                println!(
                    "observed_only pool={pool_address:#x} family={} txs={} logs={} reason={}",
                    row.family, row.txs, row.swap_logs, err
                );
            }
        }
    }

    println!();
    println!(
        "summary imported_or_importable={imported} observed_only={observed_only} already_registered={already_registered} failed_or_unsupported={failed}"
    );
    if let Some(path) = cli.output.as_deref() {
        flush_report_output()?;
        std::println!("observed pool import report written to {}", path.display());
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
    let mut min_pool_txs = 100_i64;
    let mut pool_limit = 200_i64;
    let mut apply = false;
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
            "--days" => days = iter.next().context("missing value for --days")?.parse()?,
            "--from-block" => {
                from_block = Some(
                    iter.next()
                        .context("missing value for --from-block")?
                        .parse()?,
                );
            }
            "--to-block" => {
                to_block = Some(
                    iter.next()
                        .context("missing value for --to-block")?
                        .parse()?,
                );
            }
            "--min-pool-txs" => {
                min_pool_txs = iter
                    .next()
                    .context("missing value for --min-pool-txs")?
                    .parse()?;
            }
            "--pool-limit" => {
                pool_limit = iter
                    .next()
                    .context("missing value for --pool-limit")?
                    .parse()?
            }
            "--output" => {
                output = Some(PathBuf::from(
                    iter.next().context("missing value for --output")?,
                ))
            }
            "--apply" => apply = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if min_pool_txs < 1 {
        bail!("--min-pool-txs must be positive");
    }
    if pool_limit < 1 {
        bail!("--pool-limit must be positive");
    }

    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        from_block,
        to_block,
        min_pool_txs,
        pool_limit,
        apply,
        output,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin import_observed_pools -- --address <collector> [--days 30] [--min-pool-txs 100] [--pool-limit 200] [--output report.txt] [--apply]"
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

async fn fetch_pool_hits(
    pool: &PgPool,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<PoolHit>> {
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT lower(tx_hash) AS tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
        ),
        swap_logs AS (
            SELECT
                lower(log->>'address') AS pool_address,
                lower(log->'topics'->>0) AS topic0,
                lower(ot.tx_hash) AS tx_hash,
                ot.block_number
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            WHERE lower(log->'topics'->>0) = ANY($4)
              AND ot.block_number BETWEEN $2 AND $3
        )
        SELECT
            sl.pool_address,
            sl.topic0,
            CASE sl.topic0
                WHEN $5 THEN 'v3/slipstream'
                WHEN $6 THEN 'pancake-v3'
                WHEN $7 THEN 'classic-v2'
                ELSE sl.topic0
            END AS family,
            COUNT(*)::bigint AS swap_logs,
            COUNT(DISTINCT sl.tx_hash)::bigint AS txs,
            MIN(sl.block_number)::bigint AS first_block,
            MAX(sl.block_number)::bigint AS latest_block,
            tp.symbol AS registry_symbol
        FROM swap_logs sl
        LEFT JOIN pools p ON lower(p.pool_address) = sl.pool_address
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        GROUP BY sl.pool_address, sl.topic0, tp.symbol
        HAVING COUNT(DISTINCT sl.tx_hash) >= $8
        ORDER BY COUNT(DISTINCT sl.tx_hash) DESC, COUNT(*) DESC
        LIMIT $9
        "#,
    )
    .bind(seed)
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
    .bind(cli.min_pool_txs)
    .bind(cli.pool_limit)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| PoolHit {
            pool_address: row.get("pool_address"),
            topic0: row.get("topic0"),
            family: row.get("family"),
            swap_logs: row.get("swap_logs"),
            txs: row.get("txs"),
            first_block: row.get("first_block"),
            latest_block: row.get("latest_block"),
            registry_symbol: row.get("registry_symbol"),
        })
        .collect())
}

async fn upsert_observed(
    store: &PostgresStore,
    chain_id: u64,
    row: &PoolHit,
    pool_address: Address,
    discovered: Option<&DiscoveredPool>,
    symbol: Option<&str>,
    import_status: &str,
    import_reason: Option<&str>,
) -> Result<()> {
    let state = discovered.map(|discovered| &discovered.state);
    store
        .upsert_observed_pool(
            chain_id,
            pool_address,
            &row.topic0,
            &row.family,
            state.map(|state| state.token0),
            state.map(|state| state.token1),
            symbol,
            discovered.and_then(|discovered| discovered.factory_address),
            state.map(|state| dex_to_string(state.dex)),
            state.map(|state| variant_to_string(state.variant)),
            state.map(|state| state.fee_bps),
            state.and_then(|state| state.fee_pips),
            discovered.and_then(|discovered| discovered.tick_spacing),
            discovered.and_then(|discovered| discovered.stable),
            row.txs,
            row.swap_logs,
            Some(row.first_block),
            Some(row.latest_block),
            "manual_import",
            import_status,
            import_reason,
        )
        .await
}

async fn upsert_token_registry(
    pool: &PgPool,
    chain_id: u64,
    token: Address,
    symbol: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO tokens (id, chain_id, token_address, symbol, enabled, created_at, updated_at)
        VALUES (uuid_generate_v4(), $1, $2, $3, TRUE, NOW(), NOW())
        ON CONFLICT (chain_id, token_address)
        DO UPDATE SET symbol = EXCLUDED.symbol, enabled = TRUE, updated_at = NOW()
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(format!("{token:#x}").to_ascii_lowercase())
    .bind(symbol)
    .execute(pool)
    .await?;
    Ok(())
}

async fn pair_symbol(provider: &ChainProvider, token0: Address, token1: Address) -> String {
    let token0_symbol = provider
        .fetch_erc20_symbol(token0)
        .await
        .unwrap_or_else(|_| short_addr(token0));
    let token1_symbol = provider
        .fetch_erc20_symbol(token1)
        .await
        .unwrap_or_else(|_| short_addr(token1));
    format!("{token0_symbol}/{token1_symbol}")
}

fn canonical_pair(a: Address, b: Address) -> (Address, Address) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

fn short_addr(address: Address) -> String {
    let value = format!("{address:#x}");
    value.chars().take(8).collect()
}

fn dex_to_string(dex: DexKind) -> &'static str {
    match dex {
        DexKind::Aerodrome => "Aerodrome",
        DexKind::UniswapV3 => "UniswapV3",
        DexKind::PancakeSwap => "PancakeSwap",
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
        PoolVariant::PancakeV3 => "PancakeV3",
    }
}

fn write_report_line(args: std::fmt::Arguments<'_>) {
    let line = format!("{args}");
    if let Some(output) = REPORT_OUTPUT.get() {
        let mut guard = output.lock().expect("report output lock poisoned");
        if let Some(file) = guard.as_mut() {
            let _ = writeln!(file, "{line}");
            return;
        }
    }
    std::println!("{line}");
}

fn set_report_output(path: &Path) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("failed to create output report {}", path.display()))?;
    let output = REPORT_OUTPUT.get_or_init(|| Mutex::new(None));
    *output.lock().expect("report output lock poisoned") = Some(file);
    Ok(())
}

fn flush_report_output() -> Result<()> {
    if let Some(output) = REPORT_OUTPUT.get() {
        if let Some(file) = output.lock().expect("report output lock poisoned").as_mut() {
            file.flush()?;
        }
    }
    Ok(())
}
