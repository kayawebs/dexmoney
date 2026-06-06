use std::{
    collections::{BTreeMap, BTreeSet},
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
const AERODROME_CLASSIC_FACTORY: &str = "0x420dd381b31aef6683db6b902084cb0ffece40da";
const AERODROME_SLIPSTREAM_FACTORIES: [&str; 2] = [
    "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a",
    "0xade65c38cd4849adba595a4323a8c7ddfe89716a",
];
const PANCAKE_V3_FACTORY: &str = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865";
const UNISWAP_V3_FACTORY: &str = "0x33128a8fc17869897dce68ed026d694621f6fdfd";

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
    top_counterparties: i64,
    min_txs: i64,
    pool_limit: i64,
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
    registry_dex: Option<String>,
    registry_variant: Option<String>,
    pool_enabled: Option<bool>,
    pair_enabled: Option<bool>,
    latest_state_block: Option<i64>,
    latest_state_source: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedPool {
    pool: Address,
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

#[derive(Debug, Clone)]
struct ResolvedPoolHit {
    row: PoolHit,
    resolved: ResolvedPool,
    pair: String,
    status: &'static str,
}

#[derive(Debug, Default)]
struct PairSummary {
    pools: usize,
    pool_txs_sum: i64,
    logs: i64,
    ready_pools: usize,
    missing_registry_pools: usize,
    missing_state_pools: usize,
    disabled_pools: usize,
    protocols: BTreeSet<String>,
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

    print_scope(&cli, from_block, to_block)?;
    print_counterparty_ladder(&store.pool, &cli, from_block, to_block).await?;
    print_seed_pool_coverage(&store.pool, &provider, &settings, &cli, from_block, to_block).await?;
    print_counterparty_cached_pool_coverage(&store.pool, &provider, &settings, &cli, from_block, to_block).await?;
    print_next_commands(&store.pool, &cli, from_block, to_block).await?;

    if let Some(path) = cli.output.as_deref() {
        flush_report_output()?;
        std::println!("competitor trace report written to {}", path.display());
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
    let mut top_counterparties = 20_i64;
    let mut min_txs = 10_i64;
    let mut pool_limit = 100_i64;
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
            "--top-counterparties" => {
                top_counterparties = iter
                    .next()
                    .context("missing value for --top-counterparties")?
                    .parse()
                    .context("invalid --top-counterparties")?;
            }
            "--min-txs" => {
                min_txs = iter
                    .next()
                    .context("missing value for --min-txs")?
                    .parse()
                    .context("invalid --min-txs")?;
            }
            "--pool-limit" => {
                pool_limit = iter
                    .next()
                    .context("missing value for --pool-limit")?
                    .parse()
                    .context("invalid --pool-limit")?;
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

    if top_counterparties < 1 {
        bail!("--top-counterparties must be positive");
    }
    if min_txs < 1 {
        bail!("--min-txs must be positive");
    }
    if pool_limit < 1 {
        bail!("--pool-limit must be positive");
    }

    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        from_block,
        to_block,
        top_counterparties,
        min_txs,
        pool_limit,
        output,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_trace -- --address <collector> [--days 30] [--top-counterparties 20] [--min-txs 10] [--pool-limit 100] [--output report.txt]"
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

fn print_scope(cli: &Cli, from_block: u64, to_block: u64) -> Result<()> {
    println!("== Scope ==");
    println!("collector: {:#x}", cli.address);
    println!("blocks: {from_block}..{to_block}");
    println!(
        "days: {} top_counterparties: {} min_txs: {} pool_limit: {}",
        cli.days, cli.top_counterparties, cli.min_txs, cli.pool_limit
    );
    println!();
    Ok(())
}

async fn print_counterparty_ladder(
    pool: &PgPool,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = sqlx::query(
        r#"
        WITH inbound AS (
            SELECT DISTINCT
                lower(t.counterparty) AS counterparty,
                lower(t.token_address) AS token_address,
                lower(t.tx_hash) AS tx_hash,
                t.block_number
            FROM observed_address_transfers t
            WHERE lower(t.seed_address) = lower($1)
              AND t.direction = 'in'
              AND t.block_number BETWEEN $2 AND $3
        ),
        direct_swaps AS (
            SELECT
                i.tx_hash,
                COUNT(*) FILTER (WHERE lower(log->'topics'->>0) = ANY($4))::bigint AS swap_logs,
                COUNT(DISTINCT lower(log->>'address')) FILTER (WHERE lower(log->'topics'->>0) = ANY($4))::bigint AS swap_pools,
                COUNT(DISTINCT lower(log->>'address')) FILTER (
                    WHERE lower(log->'topics'->>0) = ANY($4)
                      AND p.pool_address IS NULL
                )::bigint AS unknown_pools
            FROM inbound i
            JOIN observed_transactions ot ON lower(ot.tx_hash) = i.tx_hash
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            LEFT JOIN pools p ON lower(p.pool_address) = lower(log->>'address')
            GROUP BY i.tx_hash
        )
        SELECT
            i.counterparty,
            COUNT(DISTINCT i.tx_hash)::bigint AS inbound_txs,
            COUNT(*)::bigint AS inbound_transfers,
            COUNT(DISTINCT i.token_address)::bigint AS tokens_paid,
            COUNT(DISTINCT i.tx_hash) FILTER (WHERE COALESCE(ds.swap_logs, 0) > 0)::bigint AS direct_swap_txs,
            COALESCE(SUM(ds.swap_logs), 0)::bigint AS direct_swap_logs,
            COALESCE(SUM(ds.swap_pools), 0)::bigint AS direct_swap_pool_hits,
            COALESCE(SUM(ds.unknown_pools), 0)::bigint AS direct_unknown_pool_hits,
            MIN(i.block_number)::bigint AS first_block,
            MAX(i.block_number)::bigint AS latest_block
        FROM inbound i
        LEFT JOIN direct_swaps ds ON ds.tx_hash = i.tx_hash
        GROUP BY i.counterparty
        ORDER BY inbound_txs DESC, direct_swap_txs DESC
        LIMIT $5
        "#,
    )
    .bind(seed)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .bind(swap_topics())
    .bind(cli.top_counterparties)
    .fetch_all(pool)
    .await?;

    println!("== Collector Inbound Counterparties ==");
    if rows.is_empty() {
        println!("no cached inbound transfers; run competitor_report --hydrate-all for collector first");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "counterparty={} inbound_txs={} transfers={} tokens_paid={} direct_swap_txs={} direct_swap_logs={} direct_pool_hits={} direct_unknown_pool_hits={} blocks={}..{}",
            cell_string(&row, "counterparty").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "inbound_txs").unwrap_or_default(),
            cell_i64(&row, "inbound_transfers").unwrap_or_default(),
            cell_i64(&row, "tokens_paid").unwrap_or_default(),
            cell_i64(&row, "direct_swap_txs").unwrap_or_default(),
            cell_i64(&row, "direct_swap_logs").unwrap_or_default(),
            cell_i64(&row, "direct_swap_pool_hits").unwrap_or_default(),
            cell_i64(&row, "direct_unknown_pool_hits").unwrap_or_default(),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
        );
    }
    println!();
    Ok(())
}

async fn print_seed_pool_coverage(
    pool: &PgPool,
    provider: &ChainProvider,
    settings: &Settings,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = fetch_pool_hits(
        pool,
        r#"
        WITH related AS (
            SELECT DISTINCT lower(tx_hash) AS tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
        )
        "#,
        &seed,
        from_block,
        to_block,
        cli.min_txs,
        cli.pool_limit,
    )
    .await?;
    print_pool_hits("== Collector Related Swap Pool Coverage ==", provider, settings, rows).await
}

async fn print_counterparty_cached_pool_coverage(
    pool: &PgPool,
    provider: &ChainProvider,
    settings: &Settings,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = fetch_pool_hits(
        pool,
        r#"
        WITH top_counterparties AS (
            SELECT lower(counterparty) AS address, COUNT(DISTINCT tx_hash) AS txs
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND direction = 'in'
              AND block_number BETWEEN $2 AND $3
            GROUP BY lower(counterparty)
            ORDER BY txs DESC
            LIMIT $8
        ),
        related AS (
            SELECT DISTINCT lower(ot.tx_hash) AS tx_hash
            FROM observed_transactions ot
            JOIN top_counterparties tc
              ON lower(ot.from_address) = tc.address
              OR lower(ot.to_address) = tc.address
            WHERE ot.block_number BETWEEN $2 AND $3
        )
        "#,
        &seed,
        from_block,
        to_block,
        cli.min_txs,
        cli.pool_limit,
    )
    .await?;
    print_pool_hits(
        "== Top Counterparty Cached Tx Pool Coverage ==",
        provider,
        settings,
        rows,
    )
    .await
}

async fn fetch_pool_hits(
    pool: &PgPool,
    related_cte: &str,
    seed: &str,
    from_block: u64,
    to_block: u64,
    min_txs: i64,
    limit: i64,
) -> Result<Vec<PoolHit>> {
    let sql = format!(
        r#"
        {related_cte},
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
            END AS family,
            COUNT(*)::bigint AS swap_logs,
            COUNT(DISTINCT sl.tx_hash)::bigint AS txs,
            MIN(sl.block_number)::bigint AS first_block,
            MAX(sl.block_number)::bigint AS latest_block,
            tp.symbol AS registry_symbol,
            p.dex AS registry_dex,
            p.variant AS registry_variant,
            p.enabled AS pool_enabled,
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
        HAVING COUNT(DISTINCT sl.tx_hash) >= $9
        ORDER BY COUNT(DISTINCT sl.tx_hash) DESC, COUNT(*) DESC
        LIMIT $10
        "#
    );
    let rows = sqlx::query(&sql)
        .bind(seed)
        .bind(i64::try_from(from_block)?)
        .bind(i64::try_from(to_block)?)
        .bind(swap_topics())
        .bind(UNI_V3_SWAP_TOPIC)
        .bind(PANCAKE_V3_SWAP_TOPIC)
        .bind(CLASSIC_SWAP_TOPIC)
        .bind(limit)
        .bind(min_txs)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| PoolHit {
            pool_address: cell_string(&row, "pool_address").unwrap_or_default(),
            topic0: cell_string(&row, "topic0").unwrap_or_default(),
            family: cell_string(&row, "family").unwrap_or_default(),
            swap_logs: cell_i64(&row, "swap_logs").unwrap_or_default(),
            txs: cell_i64(&row, "txs").unwrap_or_default(),
            first_block: cell_i64(&row, "first_block").unwrap_or_default(),
            latest_block: cell_i64(&row, "latest_block").unwrap_or_default(),
            registry_symbol: cell_string(&row, "registry_symbol"),
            registry_dex: cell_string(&row, "registry_dex"),
            registry_variant: cell_string(&row, "registry_variant"),
            pool_enabled: cell_bool(&row, "pool_enabled"),
            pair_enabled: cell_bool(&row, "pair_enabled"),
            latest_state_block: cell_i64(&row, "latest_state_block"),
            latest_state_source: cell_string(&row, "latest_state_source"),
        })
        .collect())
}

async fn print_pool_hits(
    title: &str,
    provider: &ChainProvider,
    settings: &Settings,
    rows: Vec<PoolHit>,
) -> Result<()> {
    println!("{title}");
    if rows.is_empty() {
        println!("no cached swap pools matched this scope");
        println!();
        return Ok(());
    }

    let mut resolved_hits = Vec::with_capacity(rows.len());
    for row in rows {
        let pool_address = Address::from_str(&row.pool_address)
            .with_context(|| format!("invalid pool address {}", row.pool_address))?;
        let resolved = match resolve_pool(provider, settings, pool_address, &row.topic0).await {
            Ok(value) => value,
            Err(err) => ResolvedPool {
                pool: pool_address,
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
        resolved_hits.push(ResolvedPoolHit {
            row,
            resolved,
            pair,
            status,
        });
    }

    print_pair_summary(&resolved_hits);
    println!("-- Pool Details --");

    for hit in resolved_hits {
        println!(
            "status={} pool={:#x} txs={} logs={} blocks={}..{} family={} pair={} token0={} token1={} factory={} inferred={}:{} fee_pips={} fee_bps={} tick_spacing={} stable={} registry_symbol={} registry={}:{} pair_enabled={} pool_enabled={} state_block={} state_source={} error={}",
            hit.status,
            hit.resolved.pool,
            hit.row.txs,
            hit.row.swap_logs,
            hit.row.first_block,
            hit.row.latest_block,
            hit.row.family,
            hit.pair,
            fmt_addr(hit.resolved.token0),
            fmt_addr(hit.resolved.token1),
            fmt_addr(hit.resolved.factory),
            hit.resolved.inferred_dex,
            hit.resolved.inferred_variant,
            fmt_opt_u32(hit.resolved.fee_pips),
            fmt_opt_u32(hit.resolved.fee_bps),
            fmt_opt_i32(hit.resolved.tick_spacing),
            fmt_opt_bool(hit.resolved.stable),
            hit.row.registry_symbol.as_deref().unwrap_or("-"),
            hit.row.registry_dex.as_deref().unwrap_or("-"),
            hit.row.registry_variant.as_deref().unwrap_or("-"),
            fmt_opt_bool(hit.row.pair_enabled),
            fmt_opt_bool(hit.row.pool_enabled),
            hit.row.latest_state_block
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into()),
            hit.row.latest_state_source.as_deref().unwrap_or("-"),
            hit.resolved.resolve_error.as_deref().unwrap_or("-"),
        );
    }
    println!();
    Ok(())
}

fn print_pair_summary(hits: &[ResolvedPoolHit]) {
    let mut summaries = BTreeMap::<String, PairSummary>::new();
    for hit in hits {
        let summary = summaries.entry(hit.pair.clone()).or_default();
        summary.pools += 1;
        summary.pool_txs_sum += hit.row.txs;
        summary.logs += hit.row.swap_logs;
        summary
            .protocols
            .insert(format!("{}:{}", hit.resolved.inferred_dex, hit.resolved.inferred_variant));
        match hit.status {
            "ready" => summary.ready_pools += 1,
            "missing_registry" => summary.missing_registry_pools += 1,
            "missing_state" => summary.missing_state_pools += 1,
            "disabled" => summary.disabled_pools += 1,
            _ => {}
        }
    }

    let mut summaries = summaries.into_iter().collect::<Vec<_>>();
    summaries.sort_by(|(_, left), (_, right)| {
        right
            .pool_txs_sum
            .cmp(&left.pool_txs_sum)
            .then_with(|| right.pools.cmp(&left.pools))
    });

    println!("-- Pair Summary --");
    for (pair, summary) in summaries {
        let protocols = summary.protocols.into_iter().collect::<Vec<_>>().join(",");
        println!(
            "pair={} pools={} pool_txs_sum={} logs={} ready={} missing_registry={} missing_state={} disabled={} protocols={}",
            pair,
            summary.pools,
            summary.pool_txs_sum,
            summary.logs,
            summary.ready_pools,
            summary.missing_registry_pools,
            summary.missing_state_pools,
            summary.disabled_pools,
            protocols,
        );
    }
    println!();
}

async fn print_next_commands(
    pool: &PgPool,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let rows = sqlx::query(
        r#"
        SELECT lower(counterparty) AS counterparty, COUNT(DISTINCT tx_hash)::bigint AS txs
        FROM observed_address_transfers
        WHERE lower(seed_address) = lower($1)
          AND direction = 'in'
          AND block_number BETWEEN $2 AND $3
        GROUP BY lower(counterparty)
        ORDER BY txs DESC
        LIMIT $4
        "#,
    )
    .bind(seed)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .bind(cli.top_counterparties)
    .fetch_all(pool)
    .await?;

    println!("== Suggested Deeper Hydration Commands ==");
    println!("Run these only for counterparties that show direct_swap_txs=0 or cached tx coverage is empty.");
    for row in rows {
        let counterparty = cell_string(&row, "counterparty").unwrap_or_else(|| "-".into());
        println!(
            "txs={} cmd=cargo run -p base-arb-recorder --bin competitor_report -- --address {} --days {} --hydrate-all --hydrate-peer-blocks 50 --output /tmp/competitor-{}.txt",
            cell_i64(&row, "txs").unwrap_or_default(),
            counterparty,
            cli.days,
            counterparty.trim_start_matches("0x").chars().take(8).collect::<String>(),
        );
    }
    println!();
    Ok(())
}

async fn resolve_pool(
    provider: &ChainProvider,
    settings: &Settings,
    pool: Address,
    topic0: &str,
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
    let factory_hex = factory.map(|address| format!("{address:#x}"));
    let is_factory = |target: &str| {
        factory_hex
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case(target))
            .unwrap_or(false)
    };

    if topic0 == PANCAKE_V3_SWAP_TOPIC || is_factory(PANCAKE_V3_FACTORY) {
        return ("PancakeSwap".into(), "PancakeV3".into());
    }
    if topic0 == CLASSIC_SWAP_TOPIC {
        if is_factory(AERODROME_CLASSIC_FACTORY) {
            return ("Aerodrome".into(), "AerodromeVolatile".into());
        }
        return ("ClassicV2Compatible".into(), "ClassicV2".into());
    }
    if factory == settings.aerodrome_slipstream_factory {
        return ("Aerodrome".into(), "AerodromeSlipstream".into());
    }
    if AERODROME_SLIPSTREAM_FACTORIES
        .iter()
        .any(|target| is_factory(target))
    {
        return ("Aerodrome".into(), "AerodromeSlipstream".into());
    }
    if factory == settings.uniswap_v3_factory {
        return ("UniswapV3".into(), "UniswapV3".into());
    }
    if is_factory(UNISWAP_V3_FACTORY) {
        return ("UniswapV3".into(), "UniswapV3".into());
    }
    ("V3Compatible".into(), "V3".into())
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

fn coverage_status(row: &PoolHit) -> &'static str {
    match (
        row.pool_enabled,
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

fn swap_topics() -> Vec<String> {
    vec![
        UNI_V3_SWAP_TOPIC.to_string(),
        PANCAKE_V3_SWAP_TOPIC.to_string(),
        CLASSIC_SWAP_TOPIC.to_string(),
    ]
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
