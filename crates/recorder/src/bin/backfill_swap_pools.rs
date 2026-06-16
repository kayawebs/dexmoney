use std::{
    collections::{HashMap, HashSet},
    env,
    str::FromStr,
};

use alloy_primitives::Address;
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, DiscoveredPool, PoolVariant},
};
use base_arb_storage::postgres::{FactoryRegistryRecord, PostgresStore};
use serde_json::{json, Value};
use tracing_subscriber::EnvFilter;

const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;
const DEFAULT_CHUNK_BLOCKS: u64 = 1_000;
const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";

#[derive(Debug, Clone)]
struct Cli {
    days: u64,
    from_block: Option<u64>,
    to_block: Option<u64>,
    chunk_blocks: u64,
    apply: bool,
    max_logs: Option<usize>,
    pool_limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct SwapLog {
    pool: Address,
    topic0: String,
    block_number: u64,
    tx_hash: Option<String>,
}

#[derive(Debug, Clone)]
struct PoolObservation {
    pool: Address,
    topic0: String,
    first_block: u64,
    latest_block: u64,
    logs: u64,
    txs: HashSet<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let provider = ChainProvider::from_settings(&settings);
    let store = PostgresStore::connect(&settings.postgres_url).await?;

    let trusted = trusted_factories_by_address(&store, settings.chain_id).await?;
    let latest = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest);
    let from_block = cli.from_block.unwrap_or_else(|| {
        to_block.saturating_sub(cli.days.saturating_mul(APPROX_BASE_BLOCKS_PER_DAY))
    });
    if from_block > to_block {
        bail!("from block {from_block} is greater than to block {to_block}");
    }

    println!("== Global Swap Pool Backfill ==");
    println!("blocks: {from_block}..{to_block}");
    println!("chunk_blocks: {}", cli.chunk_blocks);
    println!("mode: {}", if cli.apply { "apply" } else { "dry-run" });
    println!("trusted_factories: {}", trusted.len());

    let observations = collect_swap_observations(&provider, from_block, to_block, &cli).await?;
    println!("unique_pools: {}", observations.len());

    let mut rows = observations.into_values().collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        b.logs
            .cmp(&a.logs)
            .then_with(|| b.latest_block.cmp(&a.latest_block))
            .then_with(|| format!("{:#x}", a.pool).cmp(&format!("{:#x}", b.pool)))
    });
    if let Some(limit) = cli.pool_limit {
        rows.truncate(limit);
    }

    let mut imported = 0usize;
    let mut observed_only = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    for observation in rows {
        match process_observed_pool(
            &provider,
            &store,
            &settings,
            &trusted,
            &observation,
            cli.apply,
        )
        .await
        {
            Ok(BackfillOutcome::Imported) => imported += 1,
            Ok(BackfillOutcome::ObservedOnly) => observed_only += 1,
            Ok(BackfillOutcome::Skipped) => skipped += 1,
            Err(err) => {
                failed += 1;
                eprintln!(
                    "failed pool={:#x} topic0={} logs={} latest_block={}: {err:#}",
                    observation.pool,
                    observation.topic0,
                    observation.logs,
                    observation.latest_block
                );
            }
        }
    }

    println!(
        "summary imported={imported} observed_only={observed_only} skipped={skipped} failed={failed}"
    );
    Ok(())
}

async fn collect_swap_observations(
    provider: &ChainProvider,
    from_block: u64,
    to_block: u64,
    cli: &Cli,
) -> Result<HashMap<Address, PoolObservation>> {
    let mut observations = HashMap::<Address, PoolObservation>::new();
    let mut scanned_logs = 0usize;
    let mut cursor = from_block;
    while cursor <= to_block {
        let end = cursor
            .saturating_add(cli.chunk_blocks.saturating_sub(1))
            .min(to_block);
        let logs = fetch_swap_logs(provider, cursor, end).await?;
        for raw in logs {
            if cli.max_logs.is_some_and(|limit| scanned_logs >= limit) {
                break;
            }
            scanned_logs += 1;
            let log = match parse_swap_log(raw) {
                Ok(log) => log,
                Err(err) => {
                    eprintln!("skip swap log parse error: {err}");
                    continue;
                }
            };
            let entry = observations
                .entry(log.pool)
                .or_insert_with(|| PoolObservation {
                    pool: log.pool,
                    topic0: log.topic0.clone(),
                    first_block: log.block_number,
                    latest_block: log.block_number,
                    logs: 0,
                    txs: HashSet::new(),
                });
            entry.first_block = entry.first_block.min(log.block_number);
            entry.latest_block = entry.latest_block.max(log.block_number);
            entry.logs = entry.logs.saturating_add(1);
            if let Some(tx_hash) = log.tx_hash {
                entry.txs.insert(tx_hash);
            }
        }
        if cli.max_logs.is_some_and(|limit| scanned_logs >= limit) {
            break;
        }
        cursor = end.saturating_add(1);
    }
    println!("scanned_logs: {scanned_logs}");
    Ok(observations)
}

async fn fetch_swap_logs(
    provider: &ChainProvider,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    let params = json!([{
        "fromBlock": format!("0x{from_block:x}"),
        "toBlock": format!("0x{to_block:x}"),
        "topics": [[
            V3_SWAP_TOPIC,
            PANCAKE_V3_SWAP_TOPIC,
            CLASSIC_SWAP_TOPIC
        ]]
    }]);
    provider
        .get_logs_raw(params)
        .await
        .with_context(|| format!("eth_getLogs global swaps {from_block}..{to_block}"))
}

async fn process_observed_pool(
    provider: &ChainProvider,
    store: &PostgresStore,
    settings: &Settings,
    trusted: &HashMap<Address, (DexKind, PoolVariant)>,
    observation: &PoolObservation,
    apply: bool,
) -> Result<BackfillOutcome> {
    let metadata = match provider
        .resolve_observed_pool_metadata(observation.pool, &observation.topic0)
        .await
    {
        Ok(metadata) => metadata,
        Err(err) => {
            println!(
                "unresolved pool={:#x} topic0={} logs={} reason={err}",
                observation.pool, observation.topic0, observation.logs
            );
            return Ok(BackfillOutcome::Skipped);
        }
    };
    let symbol = pair_symbol(provider, metadata.token0, metadata.token1).await;
    let txs = i64::try_from(
        observation
            .txs
            .len()
            .max(usize::try_from(observation.logs).unwrap_or(0)),
    )?;
    let logs = i64::try_from(observation.logs)?;
    let first_block = Some(i64::try_from(observation.first_block)?);
    let latest_block = Some(i64::try_from(observation.latest_block)?);
    let family = swap_family_for_topic(&observation.topic0);

    let Some(factory) = metadata.factory_address else {
        if apply {
            store
                .upsert_observed_pool(
                    settings.chain_id,
                    observation.pool,
                    &observation.topic0,
                    family,
                    Some(metadata.token0),
                    Some(metadata.token1),
                    Some(&symbol),
                    None,
                    None,
                    None,
                    metadata.fee_bps,
                    metadata.fee_pips,
                    metadata.tick_spacing,
                    metadata.stable,
                    txs,
                    logs,
                    first_block,
                    latest_block,
                    "backfill_swap",
                    "observed_only",
                    Some("pool factory() unavailable; cannot prove executor support"),
                )
                .await?;
        }
        println!(
            "observed pool={:#x} symbol={} family={} logs={} factory=none status=observed_only",
            observation.pool, symbol, family, observation.logs
        );
        return Ok(BackfillOutcome::ObservedOnly);
    };

    if let Some((dex, variant)) = trusted.get(&factory).copied() {
        match provider
            .resolve_pool_for_trusted_factory(observation.pool, factory, dex, variant)
            .await
        {
            Ok(discovered) => {
                if apply {
                    import_discovered_pool(
                        store,
                        settings,
                        observation.pool,
                        &observation.topic0,
                        family,
                        observation.latest_block,
                        &symbol,
                        dex,
                        variant,
                        factory,
                        discovered,
                        txs,
                        logs,
                        first_block,
                        latest_block,
                    )
                    .await?;
                }
                println!(
                    "importable pool={:#x} factory={:#x} symbol={} dex={} variant={} logs={}",
                    observation.pool,
                    factory,
                    symbol,
                    dex_to_string(dex),
                    variant_to_string(variant),
                    observation.logs
                );
                return Ok(BackfillOutcome::Imported);
            }
            Err(err) => {
                if apply {
                    record_observed_only(
                        store,
                        settings,
                        observation,
                        &metadata,
                        &symbol,
                        Some(factory),
                        Some(&format!("trusted factory pool resolve failed: {err}")),
                        txs,
                        logs,
                        first_block,
                        latest_block,
                    )
                    .await?;
                }
                println!(
                    "observed pool={:#x} factory={:#x} symbol={} logs={} status=trusted_resolve_failed reason={err}",
                    observation.pool, factory, symbol, observation.logs
                );
                return Ok(BackfillOutcome::ObservedOnly);
            }
        }
    }

    let inferred_dex = inferred_dex_for_swap_topic(&observation.topic0);
    let inferred_variant = inferred_variant_for_swap_topic(&observation.topic0);
    if apply {
        record_observed_only(
            store,
            settings,
            observation,
            &metadata,
            &symbol,
            Some(factory),
            Some("factory is not trusted; executable support requires classifier/router proof"),
            txs,
            logs,
            first_block,
            latest_block,
        )
        .await?;
        store
            .upsert_factory_registry(
                settings.chain_id,
                factory,
                inferred_dex,
                inferred_variant,
                false,
                true,
                "historical_swap_backfill",
                Some("observed from historical swap logs; not trusted for execution"),
                first_block,
                latest_block,
                0,
            )
            .await?;
    }
    println!(
        "observed pool={:#x} factory={:#x} symbol={} family={} logs={} status=untrusted_factory inferred_dex={} inferred_variant={}",
        observation.pool,
        factory,
        symbol,
        family,
        observation.logs,
        inferred_dex,
        inferred_variant
    );
    Ok(BackfillOutcome::ObservedOnly)
}

#[allow(clippy::too_many_arguments)]
async fn import_discovered_pool(
    store: &PostgresStore,
    settings: &Settings,
    pool: Address,
    topic0: &str,
    family: &str,
    block_number: u64,
    symbol: &str,
    dex: DexKind,
    variant: PoolVariant,
    factory: Address,
    discovered: DiscoveredPool,
    txs: i64,
    logs: i64,
    first_block: Option<i64>,
    latest_block: Option<i64>,
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
    store.upsert_discovered_pool(pair_id, &discovered).await?;
    store
        .upsert_observed_pool(
            settings.chain_id,
            pool,
            topic0,
            family,
            Some(discovered.state.token0),
            Some(discovered.state.token1),
            Some(symbol),
            Some(factory),
            Some(dex_to_string(dex)),
            Some(variant_to_string(variant)),
            Some(discovered.state.fee_bps),
            discovered.state.fee_pips,
            discovered.tick_spacing,
            discovered.stable,
            txs,
            logs,
            first_block,
            latest_block,
            "backfill_swap",
            "imported",
            None,
        )
        .await?;
    store
        .upsert_factory_registry(
            settings.chain_id,
            factory,
            dex_to_string(dex),
            variant_to_string(variant),
            true,
            true,
            "historical_swap_backfill",
            None,
            Some(i64::try_from(block_number)?),
            Some(i64::try_from(block_number)?),
            0,
        )
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn record_observed_only(
    store: &PostgresStore,
    settings: &Settings,
    observation: &PoolObservation,
    metadata: &base_arb_chain::provider::ObservedPoolMetadata,
    symbol: &str,
    factory: Option<Address>,
    reason: Option<&str>,
    txs: i64,
    logs: i64,
    first_block: Option<i64>,
    latest_block: Option<i64>,
) -> Result<()> {
    store
        .upsert_observed_pool(
            settings.chain_id,
            observation.pool,
            &observation.topic0,
            swap_family_for_topic(&observation.topic0),
            Some(metadata.token0),
            Some(metadata.token1),
            Some(symbol),
            factory,
            None,
            None,
            metadata.fee_bps,
            metadata.fee_pips,
            metadata.tick_spacing,
            metadata.stable,
            txs,
            logs,
            first_block,
            latest_block,
            "backfill_swap",
            "observed_only",
            reason,
        )
        .await
}

async fn trusted_factories_by_address(
    store: &PostgresStore,
    chain_id: u64,
) -> Result<HashMap<Address, (DexKind, PoolVariant)>> {
    Ok(store
        .trusted_factory_registry(chain_id)
        .await?
        .into_iter()
        .filter_map(|row: FactoryRegistryRecord| {
            let address = row.factory_address.parse::<Address>().ok()?;
            let dex = parse_dex_kind(&row.dex).ok()?;
            let variant = parse_pool_variant(&row.variant).ok()?;
            Some((address, (dex, variant)))
        })
        .collect())
}

fn parse_swap_log(raw: Value) -> Result<SwapLog> {
    let pool = raw
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("swap log missing address"))?
        .parse::<Address>()?;
    let topic0 = raw
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("swap log missing topic0"))?
        .to_ascii_lowercase();
    let block_number = raw
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .ok_or_else(|| anyhow::anyhow!("swap log missing blockNumber"))?;
    let tx_hash = raw
        .get("transactionHash")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    Ok(SwapLog {
        pool,
        topic0,
        block_number,
        tx_hash,
    })
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

fn swap_family_for_topic(topic0: &str) -> &'static str {
    match topic0 {
        V3_SWAP_TOPIC => "v3",
        PANCAKE_V3_SWAP_TOPIC => "pancake-v3",
        CLASSIC_SWAP_TOPIC => "classic-v2",
        _ => "unknown",
    }
}

fn inferred_dex_for_swap_topic(topic0: &str) -> &'static str {
    match topic0 {
        PANCAKE_V3_SWAP_TOPIC => "PancakeSwap",
        _ => "Unknown",
    }
}

fn inferred_variant_for_swap_topic(topic0: &str) -> &'static str {
    match topic0 {
        PANCAKE_V3_SWAP_TOPIC => "PancakeV3",
        V3_SWAP_TOPIC => "UniswapV3Compatible",
        CLASSIC_SWAP_TOPIC => "V2Compatible",
        _ => "Unknown",
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

fn parse_dex_kind(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        _ => bail!("unknown dex: {value}"),
    }
}

fn parse_pool_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        _ => bail!("unknown pool variant: {value}"),
    }
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillOutcome {
    Imported,
    ObservedOnly,
    Skipped,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut cli = Cli {
        days: 30,
        from_block: None,
        to_block: None,
        chunk_blocks: DEFAULT_CHUNK_BLOCKS,
        apply: false,
        max_logs: None,
        pool_limit: None,
    };
    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--days" => cli.days = parse_next(&mut iter, "--days")?,
            "--from-block" => cli.from_block = Some(parse_next(&mut iter, "--from-block")?),
            "--to-block" => cli.to_block = Some(parse_next(&mut iter, "--to-block")?),
            "--chunk-blocks" => cli.chunk_blocks = parse_next(&mut iter, "--chunk-blocks")?,
            "--max-logs" => cli.max_logs = Some(parse_next(&mut iter, "--max-logs")?),
            "--pool-limit" => cli.pool_limit = Some(parse_next(&mut iter, "--pool-limit")?),
            "--apply" => cli.apply = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if cli.chunk_blocks == 0 {
        bail!("--chunk-blocks must be greater than 0");
    }
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
        "Usage: cargo run -p base-arb-recorder --bin backfill_swap_pools -- [--days 30 | --from-block N --to-block N] [--chunk-blocks 1000] [--max-logs N] [--pool-limit N] [--apply]"
    );
}
