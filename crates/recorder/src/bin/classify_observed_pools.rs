use std::{env, str::FromStr};

use alloy_primitives::Address;
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::{ChainProvider, ObservedPoolMetadata};
use base_arb_common::{
    config::Settings,
    types::{DexKind, DiscoveredPool, PoolVariant},
};
use base_arb_storage::postgres::PostgresStore;
use sqlx::Row;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Cli {
    limit: i64,
    apply: bool,
    statuses: Vec<String>,
}

#[derive(Debug, Clone)]
struct ObservedPoolRow {
    pool_address: Address,
    topic0: String,
    family: String,
    txs_30d: i64,
    logs_30d: i64,
    first_block: Option<i64>,
    latest_block: Option<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let provider = ChainProvider::from_settings(&settings);
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    let rows = fetch_observed_pools(&store, settings.chain_id, &cli).await?;

    println!("== Observed Pool Classifier ==");
    println!("rows: {}", rows.len());
    println!("mode: {}", if cli.apply { "apply" } else { "dry-run" });
    println!("statuses: {}", cli.statuses.join(","));

    let mut imported = 0usize;
    let mut classified = 0usize;
    let mut unresolved = 0usize;
    let mut failed = 0usize;
    for row in rows {
        match classify_row(&provider, &store, &settings, &row, cli.apply).await {
            Ok(ClassifyOutcome::Imported) => imported += 1,
            Ok(ClassifyOutcome::ClassifiedObservedOnly) => classified += 1,
            Ok(ClassifyOutcome::Unresolved) => unresolved += 1,
            Err(err) => {
                failed += 1;
                eprintln!(
                    "failed pool={:#x} topic0={} txs={} reason={err:#}",
                    row.pool_address, row.topic0, row.txs_30d
                );
            }
        }
    }
    println!(
        "summary imported={imported} classified_observed_only={classified} unresolved={unresolved} failed={failed}"
    );
    Ok(())
}

async fn fetch_observed_pools(
    store: &PostgresStore,
    chain_id: u64,
    cli: &Cli,
) -> Result<Vec<ObservedPoolRow>> {
    let rows = sqlx::query(
        r#"
        SELECT pool_address, topic0, family, txs_30d, logs_30d, first_block, latest_block
        FROM observed_pools
        WHERE chain_id = $1
          AND import_status = ANY($2)
        ORDER BY txs_30d DESC, latest_block DESC NULLS LAST, updated_at DESC
        LIMIT $3
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(&cli.statuses)
    .bind(cli.limit)
    .fetch_all(&store.pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(ObservedPoolRow {
                pool_address: row
                    .get::<String, _>("pool_address")
                    .parse()
                    .context("invalid observed pool address")?,
                topic0: row.get("topic0"),
                family: row.get("family"),
                txs_30d: row.get("txs_30d"),
                logs_30d: row.get("logs_30d"),
                first_block: row.get("first_block"),
                latest_block: row.get("latest_block"),
            })
        })
        .collect()
}

async fn classify_row(
    provider: &ChainProvider,
    store: &PostgresStore,
    settings: &Settings,
    row: &ObservedPoolRow,
    apply: bool,
) -> Result<ClassifyOutcome> {
    let metadata = match provider
        .resolve_observed_pool_metadata(row.pool_address, &row.topic0)
        .await
    {
        Ok(metadata) => metadata,
        Err(err) => {
            if apply {
                store
                    .upsert_observed_pool(
                        settings.chain_id,
                        row.pool_address,
                        &row.topic0,
                        &row.family,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        row.txs_30d,
                        row.logs_30d,
                        row.first_block,
                        row.latest_block,
                        "unresolved",
                        Some(&format!("metadata probe failed: {err}")),
                    )
                    .await?;
            }
            println!(
                "unresolved pool={:#x} topic0={} txs={} reason={err}",
                row.pool_address, row.topic0, row.txs_30d
            );
            return Ok(ClassifyOutcome::Unresolved);
        }
    };
    let symbol = pair_symbol(provider, metadata.token0, metadata.token1).await;

    match provider
        .resolve_observed_pool_for_registry(settings, row.pool_address, &row.topic0)
        .await
    {
        Ok(discovered) => {
            let dex = discovered.state.dex;
            let variant = discovered.state.variant;
            let factory = discovered.factory_address;
            if apply {
                import_discovered_pool(
                    store,
                    settings,
                    row,
                    &symbol,
                    discovered,
                    "classifier_import",
                )
                .await?;
                if let Some(factory) = factory {
                    store
                        .upsert_factory_registry(
                            settings.chain_id,
                            factory,
                            dex_to_string(dex),
                            variant_to_string(variant),
                            true,
                            true,
                            "observed_pool_classifier",
                            Some("known executable factory confirmed from observed pool"),
                            row.first_block,
                            row.latest_block,
                            0,
                        )
                        .await?;
                }
            }
            println!(
                "importable pool={:#x} symbol={} dex={} variant={} factory={} txs={}",
                row.pool_address,
                symbol,
                dex_to_string(dex),
                variant_to_string(variant),
                factory
                    .map(|value| format!("{value:#x}"))
                    .unwrap_or_else(|| "-".to_string()),
                row.txs_30d
            );
            Ok(ClassifyOutcome::Imported)
        }
        Err(err) => {
            if apply {
                record_classified_observed_only(
                    store,
                    settings,
                    row,
                    &metadata,
                    &symbol,
                    Some(&format!(
                        "not executable by configured routers/factories: {err}"
                    )),
                )
                .await?;
                if let Some(factory) = metadata.factory_address {
                    store
                        .upsert_factory_registry(
                            settings.chain_id,
                            factory,
                            inferred_dex_for_topic(&row.topic0),
                            inferred_variant_for_topic(&row.topic0),
                            false,
                            true,
                            "observed_pool_classifier",
                            Some("ABI/state readable, but no configured executable router/factory proof"),
                            row.first_block,
                            row.latest_block,
                            0,
                        )
                        .await?;
                }
            }
            println!(
                "classified_observed_only pool={:#x} symbol={} factory={} family={} txs={} reason={err}",
                row.pool_address,
                symbol,
                metadata
                    .factory_address
                    .map(|value| format!("{value:#x}"))
                    .unwrap_or_else(|| "-".to_string()),
                row.family,
                row.txs_30d
            );
            Ok(ClassifyOutcome::ClassifiedObservedOnly)
        }
    }
}

async fn import_discovered_pool(
    store: &PostgresStore,
    settings: &Settings,
    row: &ObservedPoolRow,
    symbol: &str,
    discovered: DiscoveredPool,
    source: &str,
) -> Result<()> {
    store
        .upsert_token_registry(
            settings.chain_id,
            discovered.state.token0,
            symbol.split('/').next().unwrap_or_default(),
        )
        .await?;
    store
        .upsert_token_registry(
            settings.chain_id,
            discovered.state.token1,
            symbol.split('/').nth(1).unwrap_or_default(),
        )
        .await?;
    let (token0, token1) = canonical_pair(discovered.state.token0, discovered.state.token1);
    let pair_id = store
        .upsert_token_pair(settings.chain_id, token0, token1, symbol)
        .await?;
    let mut discovered = discovered;
    discovered.source = source.to_string();
    store.upsert_discovered_pool(pair_id, &discovered).await?;
    store
        .upsert_observed_pool(
            settings.chain_id,
            row.pool_address,
            &row.topic0,
            &row.family,
            Some(discovered.state.token0),
            Some(discovered.state.token1),
            Some(symbol),
            discovered.factory_address,
            Some(dex_to_string(discovered.state.dex)),
            Some(variant_to_string(discovered.state.variant)),
            Some(discovered.state.fee_bps),
            discovered.state.fee_pips,
            discovered.tick_spacing,
            discovered.stable,
            row.txs_30d,
            row.logs_30d,
            row.first_block,
            row.latest_block,
            "imported",
            None,
        )
        .await
}

async fn record_classified_observed_only(
    store: &PostgresStore,
    settings: &Settings,
    row: &ObservedPoolRow,
    metadata: &ObservedPoolMetadata,
    symbol: &str,
    reason: Option<&str>,
) -> Result<()> {
    store
        .upsert_observed_pool(
            settings.chain_id,
            row.pool_address,
            &row.topic0,
            &row.family,
            Some(metadata.token0),
            Some(metadata.token1),
            Some(symbol),
            metadata.factory_address,
            None,
            None,
            metadata.fee_bps,
            metadata.fee_pips,
            metadata.tick_spacing,
            metadata.stable,
            row.txs_30d,
            row.logs_30d,
            row.first_block,
            row.latest_block,
            "classified_observed_only",
            reason,
        )
        .await
}

async fn pair_symbol(provider: &ChainProvider, token0: Address, token1: Address) -> String {
    let token0_symbol = provider
        .fetch_erc20_symbol(token0)
        .await
        .unwrap_or_else(|_| short_address(token0));
    let token1_symbol = provider
        .fetch_erc20_symbol(token1)
        .await
        .unwrap_or_else(|_| short_address(token1));
    format!("{token0_symbol}/{token1_symbol}")
}

fn short_address(address: Address) -> String {
    let value = format!("{address:#x}");
    value.get(0..10).map(ToString::to_string).unwrap_or(value)
}

fn canonical_pair(a: Address, b: Address) -> (Address, Address) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
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

fn inferred_dex_for_topic(topic0: &str) -> &'static str {
    match topic0 {
        "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83" => "PancakeSwap",
        _ => "Unknown",
    }
}

fn inferred_variant_for_topic(topic0: &str) -> &'static str {
    match topic0 {
        "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83" => "PancakeV3",
        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" => {
            "UniswapV3Compatible"
        }
        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822" => "V2Compatible",
        _ => "Unknown",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassifyOutcome {
    Imported,
    ClassifiedObservedOnly,
    Unresolved,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut cli = Cli {
        limit: 500,
        apply: false,
        statuses: vec!["observed_only".to_string(), "unresolved".to_string()],
    };
    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--limit" => cli.limit = parse_next(&mut iter, "--limit")?,
            "--status" => cli.statuses.push(parse_next(&mut iter, "--status")?),
            "--only-status" => cli.statuses = vec![parse_next(&mut iter, "--only-status")?],
            "--apply" => cli.apply = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if cli.limit <= 0 {
        bail!("--limit must be greater than 0");
    }
    cli.statuses.sort();
    cli.statuses.dedup();
    Ok(cli)
}

fn parse_next<T: FromStr>(
    iter: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    name: &str,
) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let value = iter
        .next()
        .with_context(|| format!("missing value for {name}"))?;
    value
        .parse::<T>()
        .map_err(|err| anyhow::anyhow!("invalid {name} value {value}: {err}"))
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin classify_observed_pools -- [--limit 500] [--only-status observed_only] [--status classified_observed_only] [--apply]"
    );
}
